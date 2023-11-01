from typing import Any
from os import environ
import requests


def register_callback(session_id: str, node_id: str, node_type: str, cause: str):
    host = f"http://localhost:{environ['DAPR_HTTP_PORT']}"
    resp = requests.post(
        f"{host}/v1.0/invoke/workflow-manager/method/publish-workflow-callback",
        headers={"content-type": "application/json"},
        json={"session_id": session_id, "node_id": node_id, "node_type": node_type, "cuase": cause}
    )
    resp.raise_for_status()
    return resp.json()["id"]


def report_to_callback(data: Any, cid: str):
    resp = requests.post(
        f"{environ['INTERNAL_ENDPOINT'].strip('/')}/internal/v1/callback",
        headers={"content-type": "application/json"},
        json={"data": data, "cid": cid}
    )
    resp.raise_for_status()
    return resp.json()
