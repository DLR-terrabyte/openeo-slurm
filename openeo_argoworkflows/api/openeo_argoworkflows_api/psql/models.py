from typing import Optional
from sqlalchemy import JSON, Column
from openeo_fastapi.client.jobs import Job
from openeo_fastapi.client.psql.settings import BASE
from openeo_fastapi.client.psql.models import *

class ArgoJobORM(JobORM):

    workflowname = Column(VARCHAR, nullable=True)
    processing_parameters = Column(JSON, nullable=True)
    """The name of the argo workflow."""


class ArgoJob(Job):

    workflowname: Optional[str]
    """The name of the argo workflow."""

    processing_parameters: Optional[dict] = None
    """Additional openEO processing parameters used when submitting the SLURM job."""

    @classmethod
    def get_orm(cls):
        return ArgoJobORM


metadata = BASE.metadata
