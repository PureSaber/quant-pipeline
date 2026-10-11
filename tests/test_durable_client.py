import json

from test_durable_jobs import request

from quant_pipeline.durable_client import main


def test_submit_inspect_cancel_cli(tmp_path, capsys):
    path = tmp_path / "request.json"
    path.write_text(json.dumps(request()), encoding="utf-8")
    base = ["--database", str(tmp_path / "jobs.db")]
    assert main([*base, "submit", "--request", str(path), "--key", "key"]) == 0
    job = json.loads(capsys.readouterr().out)
    assert main([*base, "get", job["job_id"]]) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "queued"
    assert main([*base, "list", "--status", "queued"]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 1
    assert main([*base, "attempts", job["job_id"]]) == 0
    assert json.loads(capsys.readouterr().out) == []
    assert main([*base, "events", job["job_id"]]) == 0
    assert json.loads(capsys.readouterr().out)[0]["event"] == "submitted"
    assert main([*base, "cancel", job["job_id"]]) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "cancelled"
