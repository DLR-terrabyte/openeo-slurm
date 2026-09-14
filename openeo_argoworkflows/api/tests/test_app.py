import datetime
import fsspec

from fastapi.testclient import TestClient
from unittest.mock import patch

from openeo_fastapi.client.psql.engine import create
from openeo_argoworkflows_api.app import client
from openeo_argoworkflows_api.jobs import ArgoJobsRegister, UserWorkspace
from openeo_argoworkflows_api.auth import ExtendedAuthenticator
from openeo_argoworkflows_api.settings import ExtendedAppSettings

from openeo_argoworkflows_api.app import app as app_api

def test_jobs_is_argo():
    """Test the OpenEOApi and OpenEOCore classes interact as intended."""

    assert isinstance(client.jobs, ArgoJobsRegister)

def test_app_settings():

    blank_policies = ExtendedAppSettings(
        OIDC_POLICIES = ""
    )
    assert not blank_policies.OIDC_POLICIES

    single_policy = ExtendedAppSettings(
        OIDC_POLICIES = "groups, /staff"
    )
    assert single_policy.OIDC_POLICIES == ["groups,/staff"]

    multi_policy = ExtendedAppSettings(
        OIDC_POLICIES = "groups, /staff && groups, /users"
    )
    assert multi_policy.OIDC_POLICIES == ["groups,/staff", "groups,/users"]

    default_dask = ExtendedAppSettings()
    assert default_dask.DASK_WORKER_CORES == "4" 
    assert default_dask.DASK_WORKER_MEMORY == "8"

    updated_dask = ExtendedAppSettings(DASK_WORKER_CORES=6, DASK_WORKER_MEMORY="16")
    assert updated_dask.DASK_WORKER_CORES == "6"
    assert updated_dask.DASK_WORKER_MEMORY == "16"


def test_signed_urls(a_mock_user, a_mock_job, mock_settings):
    """ """

    fs = fsspec.filesystem(protocol="file")
    a_mock_job.status == "finished"

    create(a_mock_user)
    create(a_mock_job)

    workspace = UserWorkspace(
        user_id=str(a_mock_user.user_id),
        job_id=str(a_mock_job.job_id),
        root_dir=mock_settings.OPENEO_WORKSPACE_ROOT.parent / "out"
    )

    # # Ensure 'job workspace' exists
    fs.mkdir(workspace.job_directory)
    fs.mkdir(workspace.results_directory)

    original_file = str(
        mock_settings.OPENEO_WORKSPACE_ROOT.parent / "fake-process-graph.json"
    )

    fs.copy(
        original_file, str(workspace.results_directory / "fake-process-graph.json" )
    )

    test_path = f"{mock_settings.OPENEO_PREFIX}/files/{str(a_mock_job.job_id)}/RESULTS/fake-process-graph.json"

    signed = ExtendedAuthenticator.sign_url(
        test_path, "OPENEO_SIGN_KEY", str(a_mock_user.user_id), datetime.datetime.fromtimestamp(1678692590)
    )

    app = TestClient(app_api)

    resp = app.get(signed)
    assert resp.status_code == 200


def test_get_credentials(mock_settings):
    """ """

    test_path = f"{mock_settings.OPENEO_PREFIX}/credentials/oidc"

    app = TestClient(app_api)

    resp = app.get(test_path)
    assert resp.status_code == 200


def test_get_wellknown(mock_settings):
    """ """

    test_path = f"/.well-known/openeo"

    app = TestClient(app_api)

    resp = app.get(test_path)
    assert resp.status_code == 200

    test_path = f"{mock_settings.OPENEO_VERSION}/.well-known/openeo"

    app = TestClient(app_api)

    resp = app.get(test_path)
    assert resp.status_code == 200

    test_path = f"openeo/{mock_settings.OPENEO_VERSION}/.well-known/openeo"

    app = TestClient(app_api)

    resp = app.get(test_path)
    assert resp.status_code == 200


def test_processing_parameters():
    app = TestClient(app_api)

    resp = app.get(f"{client.settings.OPENEO_PREFIX}/processing_parameters")
    assert resp.status_code == 200

    data = resp.json()
    assert "create_job_parameters" in data
    assert "create_service_parameters" in data
    assert "create_synchronous_parameters" in data

    params = {p["name"]: p for p in data["create_job_parameters"]}
    assert set(params) == {
        "partition",
        "cpus_per_task",
        "memory",
        "time_limit",
    }
    assert params["partition"]["schema"] == {"type": "string"}
    assert params["cpus_per_task"]["schema"] == {"type": "integer", "minimum": 1, "maximum": 80}
    assert params["memory"]["schema"] == {"type": "integer", "minimum": 1, "maximum": 356}
    assert params["time_limit"]["schema"] == {"type": "integer", "minimum": 1, "maximum": 10080}
