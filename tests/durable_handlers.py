"""Trusted handlers used by real child-process worker tests."""

import os
import time


def echo(request, workspace):
    payload = request["payload"]
    time.sleep(payload.get("sleep", 0))
    if payload.get("fail"):
        raise RuntimeError("deliberate research failure")
    return {
        "result": {"value": payload.get("value"), "pid": os.getpid(), "workspace": str(workspace)},
        "outputs": payload.get("outputs", []),
    }


def malformed(request, workspace):
    return {"wrong": True}


def fail_once(request, workspace):
    marker = workspace.parent / "first-attempt"
    try:
        with marker.open("x") as stream:
            stream.write("failed")
    except FileExistsError:
        return {"result": {"retried": True}, "outputs": []}
    raise RuntimeError("first attempt failed")


def research_fail_once(request, workspace):
    fail_once(request, workspace)
    from quant_lab.rolling_research import worker_handler

    return worker_handler(request, workspace)


def research_delay(request, workspace):
    time.sleep(2)
    from quant_lab.rolling_research import worker_handler

    return worker_handler(request, workspace)
