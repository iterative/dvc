import os
from typing import Any, Dict

from dvc.utils.fs import makedirs


def get_config(wdir: str, mkdir=False) -> Dict[str, Any]:
    broker_path = os.path.join(wdir, "broker")
    broker_in_path = _unc_path(os.path.join(broker_path, "in"))
    broker_processed_path = _unc_path(os.path.join(broker_path, "processed"))
    result_path = os.path.join(wdir, "result")

    if mkdir:
        for path in (broker_in_path, broker_processed_path, result_path):
            makedirs(path, exist_ok=True)

    return {
        "broker_url": "filesystem://",
        "broker_transport_options": {
            "data_folder_in": broker_in_path,
            "data_folder_out": broker_in_path,
            "processed_folder": broker_processed_path,
            "store_processed": True,
        },
        "result_backend": "file://{}".format(_unc_path(result_path)),
        "result_persistent": True,
        "task_serializer": "json",
        "result_serializer": "json",
        "accept_content": ["json"],
        "imports": ("dvc.repo.experiments.queue.tasks", "dvc_task.proc.tasks"),
    }


def _unc_path(path: str) -> str:
    # Celery/Kombu URLs only take absolute filesystem paths
    # (UNC paths on windows)
    path = os.path.abspath(path)
    if os.name != "nt":
        return path
    drive, tail = os.path.splitdrive(path.replace(os.sep, "/"))
    if drive.endswith(":"):
        return f"//?/{drive}{tail}"
    return f"{drive}{tail}"
