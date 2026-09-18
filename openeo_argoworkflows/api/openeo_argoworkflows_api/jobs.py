import datetime
import fsspec
import json
import logging
import os
import io
import requests
import tarfile
import time
import uuid
import jwt

from fastapi import Depends, Response, HTTPException, responses
from typing import Optional
from pathlib import Path
from pydantic import conint, BaseModel, Extra, validator
from pystac import Collection, Link as StacLink
from sqlalchemy.exc import IntegrityError
from typing import Union
from urllib.parse import urljoin

from openeo_fastapi.api.types import Status, Error
from openeo_fastapi.api.models import JobsGetLogsResponse, JobsRequest
from openeo_fastapi.client.psql import engine
from openeo_fastapi.client.jobs import JobsRegister
from openeo_fastapi.client.auth import Authenticator, User

from openeo_argoworkflows_api.auth import ExtendedAuthenticator
from openeo_argoworkflows_api.settings import ExtendedAppSettings
from openeo_argoworkflows_api.psql.models import ArgoJob
from openeo_argoworkflows_api.tasks import submit_job, cancel_job


fs = fsspec.filesystem(protocol="file")

logger = logging.getLogger(__name__)
settings = ExtendedAppSettings()


class SlurmJobsRequest(JobsRequest):
    # Additional parameters defined by the openEO Processing Parameters Extension.
    job_options: Optional[dict] = None
    partition: Optional[str] = None
    cpus_per_task: Optional[int] = None
    memory: Optional[int] = None
    time_limit: Optional[int] = None

    @validator("cpus_per_task")
    def validate_cpus_per_task(cls, value):
        if value is not None and not 1 <= value <= settings.SLURM_CPUS_PER_TASK_MAX:
            raise ValueError(
                f"cpus_per_task must be between 1 and "
                f"{settings.SLURM_CPUS_PER_TASK_MAX}."
            )
        return value

    @validator("memory")
    def validate_memory(cls, value):
        if value is not None and not 1 <= value <= settings.SLURM_MEMORY_MAX:
            raise ValueError(
                f"memory must be between 1 and {settings.SLURM_MEMORY_MAX} GiB."
            )
        return value

    @validator("time_limit")
    def validate_time_limit(cls, value):
        if value is not None and not 1 <= value <= settings.SLURM_TIME_LIMIT_MAX:
            raise ValueError(
                f"time_limit must be between 1 and {settings.SLURM_TIME_LIMIT_MAX} minutes."
            )
        return value

    class Config:
        extra = Extra.allow


class UserWorkspace(BaseModel):

    root_dir: Path
    user_id: Union[str, uuid.UUID]
    job_id: Optional[Union[str, uuid.UUID]]

    def ensure(self, path: Path) -> Path:
        if not path.exists():
            path.mkdir(parents=True, exist_ok=True)
        return path

    @property
    def user_directory(self):
        return self.ensure(self.root_dir / str(self.user_id))
    
    @property
    def files_directory(self):
        return self.ensure(self.user_directory / "FILES")

    @property
    def job_directory(self):
        if self.job_id:
            return self.user_directory / str(self.job_id)
    
    @property
    def stac_directory(self):
        if self.job_id:
            return self.job_directory / "STAC"

    @property
    def results_directory(self):
        if self.job_id:
            return self.job_directory / "RESULTS"
    
    @property
    def results_collection_json(self):
        if self.job_id:
            return self.stac_directory / f"{self.job_id}_collection.json"


class ArgoJobsRegister(JobsRegister):

    def __init__(self, settings, links) -> None:
        super().__init__(settings, links)

    def create_job(
        self, body: SlurmJobsRequest, user: User = Depends(Authenticator.validate)
    ):
        """Create a new BatchJob.

        Args:
            body (JobsRequest): The Job Request that should be used to create the new BatchJob.
            user (User): The User returned from the Authenticator.

        Returns:
            Response: A general FastApi response to signify the changes where made as expected. Specific response
            headers need to be set in this response to ensure certain behaviours when being used by OpenEO client modules.
        """
        job_id = uuid.uuid4()

        if not body.process.id:
            auto_name_size = 16
            body.process.id = uuid.uuid4().hex[:auto_name_size].upper()

        # Define job options
        processing_parameters = {}
        if body.job_options:
            # Used for OpenEO Python Client 
            processing_parameters = body.job_options
        else:
            # Used for OpenEO Web Editor
            processing_parameters = {
                key: value
                for key, value in {
                    "partition": body.partition,
                    "cpus_per_task": body.cpus_per_task,
                    "memory": body.memory,
                    "time_limit": body.time_limit,
                }.items()
                if value is not None
            }

        # Create the job
        job = ArgoJob(
            job_id=job_id,
            process=body.process,
            status=Status.created,
            title=body.title,
            description=body.description,
            user_id=user.user_id,
            created=datetime.datetime.now(),
            processing_parameters=processing_parameters,
        )

        try:
            engine.create(create_object=job)
        except IntegrityError:
            raise HTTPException(
                status_code=500,
                detail=Error(code="Internal", message=f"The job {job.job_id} already exists."),
            )

        return Response(
            status_code=201,
            headers={
                "Location": f"{self.settings.API_DNS}{self.settings.OPENEO_PREFIX}/jobs/{job_id.__str__()}",
                "OpenEO-Identifier": job_id.__str__(),
                "access-control-allow-headers": "Accept-Ranges, Content-Encoding, Content-Range, Link, Location, OpenEO-Costs, OpenEO-Identifier",
                "access-control-expose-headers": "Accept-Ranges, Content-Encoding, Content-Range, Link, Location, OpenEO-Costs, OpenEO-Identifier",
            },
        )
    
    def start_job(
        self, job_id: uuid.UUID, user: User = Depends(Authenticator.validate)
    ):
        
        job = engine.get(get_model=ArgoJob, primary_key=job_id)

        if not job:
            raise HTTPException(
                    status_code=404,
                    detail="Job was not found for this ID",
                )
        
        if (job.status == Status.queued) or (job.status == Status.running):
                raise HTTPException(
                    status_code=400,
                    detail="Job is already in queue or running, and cannot be started again.",
                )

        job_workspace = (
            self.settings.OPENEO_WORKSPACE_ROOT
            / user.user_id.__str__()
            / job.job_id.__str__()
        )
        #try:
        #    fs.mkdir(job_workspace)
        #except Exception as e:
        #    raise HTTPException(
        #        status_code=500, detail=f"Could not create workspace for the current job."
        #    )

        # Submit job to SLURM
        slurm_job = submit_job(
            user._access_token,
            job.process.process_graph,
            str(job.job_id),
            processing_parameters=job.processing_parameters or {},
        )

        job.workflowname = slurm_job['job_id']
        job.status = "queued"
        queued = engine.modify(modify_object=job)

        if queued:
            return Response(
                status_code=202,
                content="The creation of the resource has been queued successfully.",
            )
        raise HTTPException(
            status_code=500,
            detail="The workflow server could not run the job at this time. Please try again later.",
        )
        
    def delete_job(
        self, job_id: uuid.UUID, user: User = Depends(Authenticator.validate)
    ):

        job = engine.get(get_model=ArgoJob, primary_key=job_id)
        if not job:
            raise HTTPException(404, "Job not found.")

        # cancel Slurm Job if queued or running
        if (job.status == Status.queued) or (job.status == Status.running):
            cancel_job(job.workflowname, user._access_token)

        # delete job from database
        engine.delete(delete_model=ArgoJob, primary_key=job.job_id)
        
        return Response(
            status_code=204,
            content="The resource has been deleted successfully.",
        )
    
    def stop_job(
        self, job_id: uuid.UUID, user: User = Depends(Authenticator.validate)
    ):
        job = engine.get(get_model=ArgoJob, primary_key=job_id)
        if not job:
            raise HTTPException(404, "Job not found.")

        if (job.status != Status.queued) and (job.status != Status.running):
            raise HTTPException(
                status_code=400,
                detail="The job isn't running or queued and therefore could not be canceled",
            )
        
        try:
            cancel_job(job.workflowname, user._access_token)
        except NotFound:
            logger.warning(f"Could not stop workflow {job.workflowname} for job {job.job_id}.")
        
        job.status = "created"
        engine.modify(modify_object=job)
        return Response(
            status_code=204, content="Process the job has been successfully canceled."
        )


    def logs(
        self,
        job_id: uuid.UUID
    ):
        
        job = engine.get(get_model=ArgoJob, primary_key=job_id)

        if not job:
            raise HTTPException(404, "Job not found.")

        if not job.workflowname:
            raise HTTPException(404, "No Job run found for this Job.")

        if not self.settings.S3_ACCESS_KEY or not self.settings.S3_ACCESS_SECRET:
            logger.error("S3 credentials are not configured.")
            raise HTTPException(
                status_code=500,
                detail="S3 log storage is not configured.",
            )

        workflowname = str(job.workflowname)

        stdout_key = (
            f"{self.settings.S3_LOG_PREFIX}/"
            f"{workflowname}_stdout.logfile"
        )
        stderr_key = (
            f"{self.settings.S3_LOG_PREFIX}/"
            f"{workflowname}_sterr.logfile"
        )

        try:
            s3 = fsspec.filesystem(
                "s3",
                key=self.settings.S3_ACCESS_KEY,
                secret=self.settings.S3_ACCESS_SECRET,
                client_kwargs={
                    "endpoint_url": str(self.settings.S3_ENDPOINT),
                },
            )

            logs = []

            for key in (stdout_key, stderr_key):
                path = f"{self.settings.S3_BUCKET}/{key}"

                try:
                    with s3.open(path, "rb") as file:
                        content = file.read().decode(
                            "utf-8",
                            errors="replace",
                        )
                    log = {"id": os.path.basename(key), "message": content}
                    if 'stdout' in key: 
                        log["level"] = "info"
                    else:
                        log["level"] = "error"
                    logs.append(log)

                except FileNotFoundError:
                    logger.warning(
                        "Log file not found: s3://%s/%s",
                        self.settings.S3_BUCKET,
                        key,
                    )

            if not logs:
                raise HTTPException(
                    status_code=404,
                    detail="No logs found for this Job.",
                )

            return JobsGetLogsResponse(
                logs=logs,
                links=[],
            ).dict(exclude_none=True)

        except HTTPException:
            raise

        except Exception as exc:
            logger.exception(
                "Could not retrieve logs for job %s from S3.",
                job_id,
            )
            raise HTTPException(
                status_code=500,
                detail="Could not retrieve logs for this Job.",
            ) from exc

       

    def get_results(
        self, job_id: uuid.UUID, user: User = Depends(Authenticator.validate)
    ):
        """Get the results for the BatchJob.

        Args:
            job_id (JobId): A UUID job id.
            body (JobsRequest): The Job Request that should be used to create the new BatchJob.
            user (User): The User returned from the Authenticator.

        Raises:
            HTTPException: Raises an exception with relevant status code and descriptive message of failure.

        """

        job = engine.get(get_model=ArgoJob, primary_key=job_id)

        #wspace = UserWorkspace(
        #    root_dir=self.settings.OPENEO_WORKSPACE_ROOT, user_id=str(user.user_id), job_id=str(job.job_id)
        #)

        username = jwt.decode(user._access_token, options={"verify_signature": False})['preferred_username']
        s3 = fsspec.filesystem(
                "s3",
                key=self.settings.S3_ACCESS_KEY,
                secret=self.settings.S3_ACCESS_SECRET,
                client_kwargs={
                    "endpoint_url": str(self.settings.S3_ENDPOINT),
                },
        )
        path = f"{self.settings.S3_BUCKET}/results/{username}/{job_id}/output/STAC/{job_id}_collection.json"
        with s3.open(path, "rb") as file:
             content = file.read().decode(
                 "utf-8",
                 errors="replace",
             )

        stac = json.loads(content)
        stac_collection = Collection.from_dict(stac)

        new_links = [link for link in stac_collection.links if link.rel != "item"]

        if self.settings.API_TLS:
            API_SELF_URL = f"https://{self.settings.API_DNS}"
        else:
            API_SELF_URL= f"http://{self.settings.API_DNS}"

        self_url = f"{self.settings.OPENEO_PREFIX}/jobs/{str(job.job_id)}/results"

        #for link in new_links:
        #    link._target_href = API_SELF_URL.__add__(self_url)

        # Sign urls
        #now = datetime.datetime.now().replace(microsecond=0)
        #week = datetime.timedelta(days=7)
        #expiry = now + week

        #canonical_url = API_SELF_URL.__add__(
        #    ExtendedAuthenticator.sign_url(
        #        url=self_url,
        #        key_name="OPENEO_SIGN_KEY",
        #        user_id=user.user_id,
        #        expiration_time=expiry
        #    )
        #)
        #new_links.append(StacLink(rel="canonical", target=canonical_url))

        stac_collection.links = new_links

        for value in stac_collection.assets.values():
            value.href = 'file://' + value.href
            #file_name = value.href.split("/")[-1]
            #relative_path = "/{job_id}/RESULTS/{file}".format(
            #    user_id=user, job_id=job_id, file=file_name
            #)
            #path ="{prefix}/files{path}".format(prefix=self.settings.OPENEO_PREFIX, path=relative_path)

            #value.href = API_SELF_URL.__add__(
            #    ExtendedAuthenticator.sign_url(
            #        url=path,
            #        key_name="OPENEO_SIGN_KEY",
            #        user_id=user.user_id,
            #        expiration_time=expiry
            #    )
            #)

        stac_collection.summaries.add(
            "datetime", {
                "minimum": str(stac_collection.extent.temporal.intervals[0][0]),
                "maximum": str(stac_collection.extent.temporal.intervals[0][1])
            }
        )

        stac_collection.extra_fields.update({"openeo:status": "finished"})

        return stac_collection.to_dict(transform_hrefs=False)
    

    def process_sync_job(self, body: JobsRequest = JobsRequest(), user: User = Depends(Authenticator.validate)):
        """Start the processing of a synchronous Job.

        Args:
            body (JobsRequest): The Job Request that should be used to create the new BatchJob.
            user (User): The User returned from the Authenticator.

        Raises:
            HTTPException: Raises an exception with relevant status code and descriptive message of failure.
                        
        """

        # Ensure there is a record of this sync job run
        job_id = uuid.uuid4()

        if not body.process.id:
            auto_name_size = 16
            body.process.id = uuid.uuid4().hex[:auto_name_size].upper()

        # Create the job
        job = ArgoJob(
            job_id=job_id,
            process=body.process,
            status=Status.queued,
            title=body.title,
            description=f"Synchronous execution of process graph {body.process.id}.",
            user_id=user.user_id,
            created=datetime.datetime.now(),
            synchronous=True
        )

        engine.create(create_object=job)
        slurm_job = submit_job(user._access_token, job.process.process_graph)

        job.workflowname = slurm_job['job_id']
        job.status = "queued"
        queued = engine.modify(modify_object=job)
        job_finished = False

        # Needs to wait for completion
        while not job_finished:
            job = engine.get(ArgoJob, job.job_id)
            if job.status == Status.finished:
                job_finished = True
            elif job.status == Status.error:
                raise HTTPException(
                    status_code=500,
                    detail=Error(code="InternalServerError", message="Failed to process. Submit as batch job to view logs."),
                )
            elif job.status == Status.running:
                time.sleep(15)

        wspace = UserWorkspace(
            root_dir=self.settings.OPENEO_WORKSPACE_ROOT, user_id=str(user.user_id), job_id=str(job.job_id)
        )

        files = [file for file in wspace.results_directory.glob(f"*") if file.is_file()]

        if len(files) == 0:
            raise HTTPException(
                status_code=500,
                detail=f"No files to return for request.",
            )
        elif len(files) == 1:
            def single_file_iterator(file_path):
                with open(file_path, "rb") as file:
                    yield from file
            
            file = files[0]

            # TODO Improve, maybe move general functionality of mimetypes to openeo-fastapi
            extention = os.path.splitext(file)[1]
            mime_types = {
                ".tif": "image/tiff; application=geotiff; profile=cloud-optimized",
                ".nc": "application/netcdf",
                ".json": "application/json"
            }
            mime_type = mime_types[extention]

            response = responses.StreamingResponse(
                single_file_iterator(file),
                200,
                headers={
                    "Content-Disposition": f'attachment; filename="{os.path.basename(file)}"',
                    "Content-Type": mime_type,
                },
            )

        else:
            tar_buffer = io.BytesIO()
            with tarfile.open(mode="w", fileobj=tar_buffer) as tar:
                for file_path in files:
                    tar.add(file_path, arcname=os.path.basename(file_path))

            def tar_file_iterator(tar_buffer):
                tar_buffer.seek(0)
                yield from tar_buffer

            response = responses.StreamingResponse(
                tar_file_iterator(tar_buffer),
                200,
                headers={
                    "Content-Disposition": 'attachment; filename="archive.tar"',
                    "Content-Type": "application/x-tar",
                },
            )
        return response

