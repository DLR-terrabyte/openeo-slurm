import os
import jwt
import json
import requests
from datetime import timedelta
from openeo_fastapi.api.types import Status
from typing import Any

from openeo_fastapi.client.psql.engine import modify
from openeo_argoworkflows_api.psql.models import ArgoJob
from openeo_argoworkflows_api.settings import ExtendedAppSettings

settings = ExtendedAppSettings()

def get_service_token(client_id, client_secret):
    token_response2 = requests.post(
      os.getenv('OIDC_URL') + "protocol/openid-connect/token",
      headers={"Content-Type": "application/x-www-form-urlencoded"},
      data={
        "client_id": os.getenv('CLIENT_ID'),
        "client_secret": os.getenv('CLIENT_SECRET'),
        "grant_type": "client_credentials",
        "scope": "slurmrest",
     },
    )
    token_response2.raise_for_status()
    token_data2 = token_response2.json()
    token = token_data2["access_token"]
    return token


def get_slurm_payload(process_graph, username):
    if not os.path.exists('/config/sbatch_template.sh'):
        raise HTTPException(
            status_code=500, detail=f"Could not find sbatch template file."
        )
    slurm_content = open('/config/sbatch_template.sh').read()
    slurm_content = slurm_content.replace('$PROCESS_GRAPH', process_graph)
    slurm_content = slurm_content.replace('$USER', username)
    payload = {
      "job": {
        "name": "openeo",
        "partition": os.getenv('SLURM_PARTITION_DEFAULT'),
        "standard_output": os.getenv('SLURM_JOB_STDOUT'),
        "standard_error": os.getenv('SLURM_JOB_STDERR'),
        "environment": {
          "PATH": os.getenv('SLURM_JOB_PATH'),
          "LD_LIBRARY_PATH": os.getenv('SLURM_JOB_LD_PATH')
        }
      },
      "script": slurm_content
    }
    return payload


def submit_job(access_token, process_graph):
    client_id = os.getenv('CLIENT_ID')
    client_secret = os.getenv('CLIENT_SECRET')
    username = jwt.decode(access_token, options={"verify_signature": False})['preferred_username']
    payload = get_slurm_payload(json.dumps(process_graph), username)
    headers = {"Authorization": f"Bearer {access_token},{get_service_token(client_id, client_secret)}"}
    response = requests.post(os.getenv('SLURM_REST_API') + "/job/submit", json=payload, headers=headers)
    slurm_job = response.json()
    return slurm_job


def get_job_status(job_id):
    client_id = os.getenv('CLIENT_ID_STATUS')
    client_secret = os.getenv('CLIENT_SECRET_STATUS')
    headers = {"Authorization": f"Bearer {get_service_token(client_id, client_secret)}"}
    response = requests.get(os.getenv('SLURM_REST_API') + "/job/%s" % job_id, headers=headers)
    info = response.json()
    job_status = info['jobs'][0]['job_state']
    if job_status in ["COMPLETED"]: 
       return Status.finished
    elif job_status in ["FAILED", "SUSPENDED", "PREEMPTED"]: 
       return Status.error
    elif job_status == "RUNNING":
       return Status.running
    else:
       return Status.queued
