import json

from openeo_argoworkflows_api.tasks import get_slurm_payload


def test_get_slurm_payload_processing_parameters(monkeypatch, tmp_path):
    template = tmp_path / "sbatch_template.sh"
    template.write_text("echo $PROCESS_GRAPH $USER")
    monkeypatch.setattr("openeo_argoworkflows_api.tasks.os.path.exists", lambda path: True)
    monkeypatch.setattr("builtins.open", lambda *args, **kwargs: template.open())

    monkeypatch.setenv("SLURM_PARTITION_DEFAULT", "default")
    payload = get_slurm_payload(
        {"process_graph": {}},
        "alice",
        processing_parameters={
            "partition": "long",
            "cpus_per_task": 16,
            "memory": 64,
            "time_limit": 240,
        },
    )

    assert payload["job"]["partition"] == "long"
    assert payload["job"]["cpus_per_task"] == 16
    assert payload["job"]["memory_per_node"] == 64 * 1024
    assert payload["job"]["time_limit"] == {"set": True, "number": 240}


def test_get_slurm_payload_uses_defaults(monkeypatch, tmp_path):
    template = tmp_path / "sbatch_template.sh"
    template.write_text("echo")
    monkeypatch.setattr("openeo_argoworkflows_api.tasks.os.path.exists", lambda path: True)
    monkeypatch.setattr("builtins.open", lambda *args, **kwargs: template.open())

    monkeypatch.setenv("SLURM_PARTITION_DEFAULT", "short")
    monkeypatch.setenv("SLURM_CPUS_PER_TASK_DEFAULT", "4")
    monkeypatch.setenv("SLURM_MEMORY_DEFAULT", "16")
    monkeypatch.setenv("SLURM_TIME_LIMIT_DEFAULT", "60")

    payload = get_slurm_payload({}, "alice")

    assert payload["job"]["partition"] == "short"
    assert payload["job"]["cpus_per_task"] == 4
    assert payload["job"]["memory_per_node"] == 16 * 1024
    assert payload["job"]["time_limit"] == {"set": True, "number": 60}
