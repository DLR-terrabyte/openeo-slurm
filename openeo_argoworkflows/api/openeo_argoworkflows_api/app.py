import json
from pathlib import Path

from fastapi import FastAPI, Header
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import RedirectResponse

from openeo_fastapi.api.app import OpenEOApi
from openeo_fastapi.api.types import Billing, Plan, FileFormat, GisDataType, Endpoint
from openeo_fastapi.client.core import OpenEOCore
from openeo_pg_parser_networkx.process_registry import Process as pgProcess
from openeo_fastapi.client.auth import Authenticator, AuthToken

from openeo_argoworkflows_api.jobs import ArgoJobsRegister
from openeo_argoworkflows_api.files import ArgoFileRegister
from openeo_argoworkflows_api.settings import ExtendedAppSettings


gtif = FileFormat(
   title="GTiff",
    gis_data_types=[GisDataType("raster")],
    parameters={},
)

netcdf = FileFormat(
   title="netCDF",
    gis_data_types=[GisDataType("raster")],
    parameters={},
)

input_formats = [ gtif, netcdf ]
output_formats = [ netcdf ]

links = []

settings = ExtendedAppSettings()

client = OpenEOCore(
    settings=settings,
    files=ArgoFileRegister(settings=settings, links=links),
    jobs=ArgoJobsRegister(settings=settings, links=links),
    input_formats=input_formats,
    output_formats=output_formats,
    links=links,
    billing=Billing(
        currency="credits",
        default_plan="a-cloud",
        plans=[Plan(name="user", description="Subscription plan.", paid=True)],
    )
)# Advertise the Processing Parameters Extension endpoint in GET /.
client.endpoints.append(
    Endpoint(
        path="/processing_parameters",
        methods=["GET"],
    )
)


app = FastAPI()

app.router.add_api_route(
    name="file_headers",
    path=f"/{client.settings.OPENEO_VERSION}/files" + "/{path:path}",
    response_model=None,
    response_model_exclude_unset=False,
    response_model_exclude_none=True,
    methods=["HEAD"],
    endpoint=client.files.file_header,
)

api = OpenEOApi(client=client, app=app)

# Register custom processes (not in upstream openeo-processes-dask)
_specs_dir = Path(__file__).parent / "specs"
for spec_file in _specs_dir.glob("*.json"):
    with open(spec_file) as f:
        spec = json.load(f)
    api.client.processes.process_registry[("predefined", spec["id"])] = pgProcess(spec)
# Clear the cached process list so it includes the new processes
api.client.processes.get_available_processes.cache_clear()

def validate_auth(authorization: str = Header()):
    user = Authenticator.validate(authorization)
    parsed_token = AuthToken.from_token(authorization)
    user._access_token = parsed_token.token
    return user

api.override_authentication(validate_auth)


def get_processing_parameters():
    # Implements the openEO Processing Parameters Extension 0.1.0.
    # The public units intentionally hide SLURM's low-level representation:
    # memory is GiB and time_limit is minutes.
    parameter_specs = [
        (
            "partition",
            "SLURM partition to use for the job. If omitted, the backend default partition is used.",
            {"type": "string"},
            settings.SLURM_PARTITION_DEFAULT,
        ),
        (
            "cpus_per_task",
            "Number of CPUs allocated to each SLURM task.",
            {"type": "integer", "minimum": 1, "maximum": settings.SLURM_CPUS_PER_TASK_MAX},
            settings.SLURM_CPUS_PER_TASK_DEFAULT,
        ),
        (
            "memory",
            "Memory requested for the SLURM job in GiB. The value is applied as memory per node.",
            {"type": "integer", "minimum": 1, "maximum": settings.SLURM_MEMORY_MAX},
            settings.SLURM_MEMORY_DEFAULT,
        ),
        (
            "time_limit",
            "Maximum runtime of the SLURM job in minutes.",
            {"type": "integer", "minimum": 1, "maximum": settings.SLURM_TIME_LIMIT_MAX},
            settings.SLURM_TIME_LIMIT_DEFAULT,
        ),
    ]

    parameters = []
    for name, description, schema, default in parameter_specs:
        parameter = {
            "name": name,
            "description": description,
            "optional": True,
            "schema": schema,
        }
        if default is not None:
            parameter["default"] = default
        parameters.append(parameter)

    return {
        "create_job_parameters": parameters,
        "create_service_parameters": [],
        "create_synchronous_parameters": [],
    }


api.app.router.add_api_route(
    name="processing_parameters",
    path=f"{client.settings.OPENEO_PREFIX}/processing_parameters",
    response_model=None,
    response_model_exclude_unset=False,
    response_model_exclude_none=True,
    methods=["GET"],
    endpoint=get_processing_parameters,
)

api.app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=[
        "Accept-Ranges",
        "Content-Encoding",
        "Content-Range",
        "Range",
        "Link",
        "Location",
        "OpenEO-Costs",
        "OpenEO-Identifier",
        "Authorization",
    ],
    expose_headers=[
        "Accept-Ranges",
        "Content-Encoding",
        "Content-Range",
        "Link",
        "Location",
        "OpenEO-Costs",
        "OpenEO-Identifier",
    ],
)

def redirect_wellknown():
    return RedirectResponse("/.well-known/openeo")

api.app.router.add_api_route(
    name="redirect_wellknown",
    path=f"/{client.settings.OPENEO_VERSION}/.well-known/openeo",
    response_model=None,
    response_model_exclude_unset=False,
    response_model_exclude_none=True,
    methods=["GET"],
    endpoint=redirect_wellknown,
)

api.app.router.add_api_route(
    name="redirect_wellknown",
    path=f"/openeo/{client.settings.OPENEO_VERSION}/.well-known/openeo",
    response_model=None,
    response_model_exclude_unset=False,
    response_model_exclude_none=True,
    methods=["GET"],
    endpoint=redirect_wellknown,
)

app = api.app
