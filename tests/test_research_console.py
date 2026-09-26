import json
import threading
from http.client import HTTPConnection
from http.server import ThreadingHTTPServer
from pathlib import Path

import pytest
from quant_lab.research import digest, load_recipe

from quant_pipeline.research_demo import create_demo
from quant_pipeline.research_web import ResearchConsole, make_handler, safe_child


@pytest.fixture
def console(tmp_path):
    item = ResearchConsole(tmp_path / "console", [tmp_path], start_worker=False)
    recipe = load_recipe(create_demo(tmp_path / "data"))
    item.save_recipe(recipe)
    yield item
    item.close()


def test_queue_freezes_recipe_and_resumes_idempotently(console):
    recipe = load_recipe(console.recipe_path("synthetic-etf-example"))
    first = console.submit(recipe["study_id"])
    assert console.submit(recipe["study_id"]) == first
    with pytest.raises(ValueError, match="frozen"):
        console.save_recipe(recipe)
    with console.connect() as db:
        db.execute("UPDATE jobs SET status='failed'")
    assert console.submit(recipe["study_id"]) == first
    assert console.state()["jobs"][0]["status"] == "queued"
    clone = console.clone(recipe["study_id"], "second-experiment")
    assert clone["mode"] == "exploratory"
    assert console.recipe_path(clone["study_id"]).exists()
    console.note(clone["study_id"], "保留失败研究及下一步对照")


def test_paths_and_arbitrary_input_are_rejected(console):
    with pytest.raises(ValueError):
        console.recipe_path("../../outside")
    recipe = load_recipe(console.recipe_path("synthetic-etf-example"))
    recipe["inputs"]["catalog"] = "relative.csv"
    with pytest.raises(ValueError, match="absolute"):
        console.save_recipe(recipe)
    recipe["inputs"]["catalog"] = str(console.root.anchor + "outside.csv")
    with pytest.raises(ValueError, match="outside"):
        console.save_recipe(recipe)
    with pytest.raises(ValueError):
        console.compare([])


def test_http_mutations_need_token_origin_and_bounded_json(console):
    server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(console))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    connection = HTTPConnection("127.0.0.1", server.server_port)
    try:
        connection.request("GET", "/")
        response = connection.getresponse()
        assert response.status == 200 and "研究操作台" in response.read().decode()
        connection.request(
            "POST",
            "/api/run",
            json.dumps({"study_id": "synthetic-etf-example"}),
            {"Content-Type": "application/json"},
        )
        response = connection.getresponse()
        assert response.status == 403
        response.read()
        headers = {
            "Content-Type": "application/json",
            "X-Workbench-Token": console.token,
            "Origin": "https://attacker.invalid",
        }
        connection.request("POST", "/api/run", "{}", headers)
        response = connection.getresponse()
        assert response.status == 403
        response.read()
        headers["Origin"] = f"http://127.0.0.1:{server.server_port}"
        connection.request(
            "POST", "/api/run", json.dumps({"study_id": "synthetic-etf-example"}), headers
        )
        response = connection.getresponse()
        assert response.status == 200
        job = json.loads(response.read())
        connection.request("GET", f"/files/{job['job_id']}/../../console.db")
        response = connection.getresponse()
        assert response.status == 400
        response.read()
        connection.request("GET", "/api/state", headers={"Host": "attacker.invalid"})
        response = connection.getresponse()
        assert response.status == 403
        response.read()
    finally:
        connection.close()
        server.shutdown()
        server.server_close()
        thread.join(2)


def test_restart_requires_explicit_resume_and_two_writers_cannot_share_workspace(console):
    job = console.submit("synthetic-etf-example")
    with console.connect() as db:
        db.execute("UPDATE jobs SET status='running'")
    with pytest.raises(OSError):
        ResearchConsole(console.root, console.data_roots, start_worker=False)
    console.close()
    recovered = ResearchConsole(console.root, console.data_roots, start_worker=False)
    try:
        assert recovered.job(job["job_id"])["status"] == "interrupted"
        assert recovered.submit("synthetic-etf-example") == job
        assert recovered.job(job["job_id"])["status"] == "queued"
    finally:
        recovered.close()


def test_worker_runs_only_frozen_recipe_and_preserves_failure_log(console, monkeypatch):
    from quant_pipeline import research_web

    finished = threading.Event()
    calls = []

    class Process:
        def __init__(self, command, **kwargs):
            calls.append(command)
            assert "shell" not in kwargs
            frozen = Path(command[3])
            assert frozen.parent.name == "requests"
            assert command[-2:] == ["--expected-recipe-sha256", digest(load_recipe(frozen))]
            console.recipe_path("synthetic-etf-example").write_text(
                "externally changed after queue check"
            )
            assert load_recipe(frozen)["study_id"] == "synthetic-etf-example"
            kwargs["stdout"].write("explicit data failure\n")

        def wait(self):
            return 2

    original_connect = console.connect

    def connect():
        db = original_connect()
        db.set_trace_callback(lambda query: finished.set() if "status='failed'" in query else None)
        return db

    monkeypatch.setattr(research_web.subprocess, "Popen", Process)
    monkeypatch.setattr(console, "connect", connect)
    job = console.submit("synthetic-etf-example")
    console.worker = threading.Thread(target=console._work, daemon=True)
    console.worker.start()
    assert finished.wait(5)
    console.close()
    assert console.job(job["job_id"])["status"] == "failed"
    assert calls[0][1:3] == ["-m", "quant_pipeline.research_workbench"]
    assert "data failure" in (console.root / "logs" / (job["job_id"] + ".log")).read_text()


def test_http_reads_notes_and_rejects_unknown_or_malformed_mutations(console):
    server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(console))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    connection = HTTPConnection("127.0.0.1", server.server_port)
    headers = {"Content-Type": "application/json", "X-Workbench-Token": console.token}

    def request(method, path, payload=None):
        connection.request(method, path, None if payload is None else json.dumps(payload), headers)
        response = connection.getresponse()
        return response.status, json.loads(response.read())

    try:
        assert request("GET", "/api/state")[1]["recipes"]
        assert request("GET", "/api/recipes/synthetic-etf-example")[1]["strategy"]
        assert (
            request(
                "POST", "/api/notes", {"study_id": "synthetic-etf-example", "body": "成本需要对照"}
            )[0]
            == 200
        )
        assert request("GET", "/api/notes/synthetic-etf-example")[1][0]["body"] == "成本需要对照"
        assert (
            request(
                "POST", "/api/clone", {"study_id": "synthetic-etf-example", "new_id": "clone-v2"}
            )[0]
            == 200
        )
        recipe = request("GET", "/api/recipes/clone-v2")[1]
        recipe["hypothesis"] = "new hypothesis"
        assert request("POST", "/api/recipes", {"recipe": recipe})[0] == 200
        job = request("POST", "/api/run", {"study_id": "clone-v2"})[1]
        assert request("GET", "/api/logs/" + job["job_id"])[1]["log"] == ""
        assert request("POST", "/api/compare", {"job_ids": []})[0] == 400
        assert request("POST", "/api/unknown", {"anything": True})[0] == 404
        assert request("POST", "/api/run", {})[0] == 400
        assert request("GET", "/unavailable")[0] == 404
        connection.request("POST", "/api/run", "{}", {**headers, "Content-Type": "text/plain"})
        response = connection.getresponse()
        assert response.status == 400
        response.read()
    finally:
        connection.close()
        server.shutdown()
        server.server_close()
        thread.join(2)


def test_managed_path_rejects_links_and_traversal(console, monkeypatch):
    original = Path.resolve
    redirected = console.root / "recipes" / "redirected.yaml"

    def resolve(path, *args, **kwargs):
        if path == redirected:
            return console.root.parent / "outside.yaml"
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, "resolve", resolve)
    with pytest.raises(ValueError, match="filesystem link"):
        safe_child(console.root, "recipes", redirected.name)
    with pytest.raises(ValueError):
        safe_child(console.root, "recipes", "..", "..", "outside")


def test_failed_selected_study_is_never_silently_removed_from_comparison(console, monkeypatch):
    import quant_report_hub.research_workbench

    first = console.submit("synthetic-etf-example")
    console.clone("synthetic-etf-example", "failed-control")
    second = console.submit("failed-control")

    def summary(path):
        study_id = path.parent.name
        failed = study_id == "failed-control"
        return {
            "study_id": study_id,
            "recipe": {"study_id": study_id},
            "results": [
                {
                    "candidate": {"name": "base"},
                    "status": "failed" if failed else "completed",
                    "error": "missing prices" if failed else None,
                }
            ],
        }

    monkeypatch.setattr(quant_report_hub.research_workbench, "load_study", summary)
    result = console.compare([first["job_id"], second["job_id"]])
    assert result["comparable"] is False
    assert len(result["selected_studies"]) == 2
    assert result["selected_studies"][1]["error"] == "missing prices"
