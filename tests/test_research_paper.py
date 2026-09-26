from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest
from quant_lab.research import candidates, load_recipe

from quant_pipeline import research_paper as paper
from quant_pipeline.research_demo import create_demo
from quant_pipeline.research_validation import return_metrics


@pytest.fixture
def account(tmp_path, monkeypatch):
    recipe = load_recipe(create_demo(tmp_path / "data"))
    recipe["interval"] = {"start": "2023-04-03", "end": "2023-05-30"}
    recipe["diagnostics"] = {}
    candidate = candidates(recipe)[0]
    summary = {
        "recipe": recipe,
        "definition_sha256": "study-hash",
        "results": [
            {
                "candidate": candidate,
                "status": "completed",
                "identity": {"code": "fixed"},
                "data_identity": paper.input_identity(recipe),
                "scope": "synthetic-software-demonstration",
                "metrics": {"total_return": 0.1},
            }
        ],
    }
    import quant_report_hub.research_workbench

    monkeypatch.setattr(quant_report_hub.research_workbench, "load_study", lambda _: summary)
    monkeypatch.setattr(paper, "equity_code_identity", lambda: {"code": "fixed"})
    source = tmp_path / "study.json"
    source.write_text("source study")
    root = tmp_path / "paper"
    paper.promote(
        source,
        "base",
        root,
        account_id="forward-a",
        start="2023-06-01",
        end="2023-06-05",
        now=datetime(2023, 5, 31, tzinfo=timezone.utc),
    )
    return root, recipe["inputs"]


def ledger(recipe, candidate, output):
    dates = pd.bdate_range(recipe["interval"]["start"], recipe["interval"]["end"])
    returns = pd.Series([0.0] + [0.01] * (len(dates) - 1), index=dates)
    returns.to_csv(output / "returns.csv", header=["net_return"])
    return {"metrics": return_metrics(returns), "scope": "synthetic-test"}


def test_forward_account_advance_resume_and_one_time_seal(account):
    root, inputs = account
    first = paper.observe(
        root,
        inputs,
        as_of="2023-06-01",
        now=datetime(2023, 6, 1, 10, tzinfo=timezone.utc),
        executor=ledger,
    )
    repeated = paper.observe(
        root,
        inputs,
        as_of="2023-06-01",
        now=datetime(2023, 6, 1, 11, tzinfo=timezone.utc),
        executor=ledger,
    )
    assert first == repeated
    final = paper.observe(
        root,
        inputs,
        as_of="2023-06-05",
        now=datetime(2023, 6, 5, 10, tzinfo=timezone.utc),
        executor=ledger,
    )
    assert final["metrics"]["sessions"] == 3
    with pytest.raises(ValueError, match="not ended"):
        paper.seal(root, now=datetime(2023, 6, 5, 15, tzinfo=timezone.utc))
    result = paper.seal(root, now=datetime(2023, 6, 6, tzinfo=timezone.utc))
    assert result["strategy_net"]["total_return"] == pytest.approx(0.0201)
    assert result["scope"] == "synthetic-forward-software-validation"
    with pytest.raises(ValueError, match="sealed"):
        paper.observe(
            root,
            inputs,
            as_of="2023-06-05",
            now=datetime(2023, 6, 6, tzinfo=timezone.utc),
            executor=ledger,
        )


def test_future_session_code_change_and_modified_history_block(account, monkeypatch):
    root, inputs = account
    with pytest.raises(ValueError, match="completed session"):
        paper.observe(
            root,
            inputs,
            as_of="2023-06-01",
            now=datetime(2023, 6, 1, 1, tzinfo=timezone.utc),
            executor=ledger,
        )
    paper.observe(
        root,
        inputs,
        as_of="2023-06-01",
        now=datetime(2023, 6, 1, 10, tzinfo=timezone.utc),
        executor=ledger,
    )
    monkeypatch.setattr(paper, "input_prefix", lambda *a: {"revised": True})
    with pytest.raises(ValueError, match="revised"):
        paper.observe(
            root,
            inputs,
            as_of="2023-06-02",
            now=datetime(2023, 6, 2, 10, tzinfo=timezone.utc),
            executor=ledger,
        )
    monkeypatch.setattr(paper, "equity_code_identity", lambda: {"code": "changed"})
    with pytest.raises(ValueError, match="identity"):
        paper.observe(
            root,
            inputs,
            as_of="2023-06-02",
            now=datetime(2023, 6, 2, 10, tzinfo=timezone.utc),
            executor=ledger,
        )


def test_failed_observation_preserved_and_cannot_seal_short_interval(account):
    root, inputs = account

    def fail(*_):
        raise ValueError("missing market state")

    with pytest.raises(ValueError, match="market state"):
        paper.observe(
            root,
            inputs,
            as_of="2023-06-01",
            now=datetime(2023, 6, 1, 10, tzinfo=timezone.utc),
            executor=fail,
        )
    paper.observe(
        root,
        inputs,
        as_of="2023-06-01",
        now=datetime(2023, 6, 1, 10, tzinfo=timezone.utc),
        executor=ledger,
    )
    events = paper.TrialRegistry(root / "account.db").history("forward-a")
    assert [r["status"] for r in events] == ["running", "failed", "running", "completed"]
    with pytest.raises(ValueError, match="every registered session"):
        paper.seal(root, now=datetime(2023, 6, 6, tzinfo=timezone.utc))


def test_revision_of_old_returns_fails_even_if_input_hashes_still_match(account):
    root, inputs = account
    paper.observe(
        root,
        inputs,
        as_of="2023-06-02",
        now=datetime(2023, 6, 2, 10, tzinfo=timezone.utc),
        executor=ledger,
    )

    def inconsistent(recipe, candidate, output):
        result = ledger(recipe, candidate, output)
        returns = pd.read_csv(output / "returns.csv", index_col=0)
        returns.iloc[1, 0] = 0.02
        returns.to_csv(output / "returns.csv")
        return result

    with pytest.raises(ValueError, match="previous account returns"):
        paper.observe(
            root,
            inputs,
            as_of="2023-06-05",
            now=datetime(2023, 6, 5, 10, tzinfo=timezone.utc),
            executor=inconsistent,
        )


def test_first_observation_rejects_revised_development_data_and_swapped_lineage(
    account, monkeypatch
):
    root, inputs = account
    original = paper.input_prefix
    monkeypatch.setattr(paper, "input_prefix", lambda *a: {"changed": True})
    with pytest.raises(ValueError, match="after promotion"):
        paper.observe(
            root,
            inputs,
            as_of="2023-06-01",
            now=datetime(2023, 6, 1, 10, tzinfo=timezone.utc),
            executor=ledger,
        )
    monkeypatch.setattr(paper, "input_prefix", original)
    monkeypatch.setattr(paper, "input_lineage", lambda *a: {"another": "dataset"})
    with pytest.raises(ValueError, match="lineage"):
        paper.observe(
            root,
            inputs,
            as_of="2023-06-01",
            now=datetime(2023, 6, 1, 10, tzinfo=timezone.utc),
            executor=ledger,
        )


def test_seal_recovers_file_failure_without_reevaluating(account, monkeypatch):
    root, inputs = account
    paper.observe(
        root,
        inputs,
        as_of="2023-06-05",
        now=datetime(2023, 6, 5, 10, tzinfo=timezone.utc),
        executor=ledger,
    )
    original = paper.atomic_json

    def broken(*args):
        raise OSError("disk temporarily unavailable")

    monkeypatch.setattr(paper, "atomic_json", broken)
    with pytest.raises(OSError):
        paper.seal(root, now=datetime(2023, 6, 6, tzinfo=timezone.utc))
    assert paper.sealed_evidence(root, "forward-a") is not None
    assert not (root / "evaluation.json").exists()
    monkeypatch.setattr(paper, "atomic_json", original)
    evidence = paper.seal(root, now=datetime(2023, 6, 6, tzinfo=timezone.utc))
    assert (root / "evaluation.json").exists()
    assert paper.seal(root, now=datetime(2023, 6, 7, tzinfo=timezone.utc)) == evidence
    monkeypatch.setattr(paper, "return_metrics", lambda _: {"total_return": 999})
    with pytest.raises(ValueError, match="reevaluated"):
        paper.seal(root, now=datetime(2023, 6, 7, tzinfo=timezone.utc))


def test_same_history_prefix_cannot_hide_a_different_future_provider(account, monkeypatch):
    import quant_report_hub.research_workbench
    from quant_data_kit.research_coverage import import_history

    root, _ = account
    spec = paper.load_account(root)["definition"]
    recipe = spec["recipe"]
    for suffix, provider, future in (("one", "vendor-one", "true"), ("two", "vendor-two", "false")):
        source = root.parent / (suffix + ".csv")
        pd.DataFrame(
            [
                {
                    "domain": "status",
                    "symbol": "510300",
                    "field": "tradable",
                    "value": "true",
                    "effective_at": "2023-01-02T08:00:00+08:00",
                    "available_at": "2023-01-02T08:00:00+08:00",
                },
                {
                    "domain": "status",
                    "symbol": "510300",
                    "field": "tradable",
                    "value": future,
                    "effective_at": "2023-06-01T08:00:00+08:00",
                    "available_at": "2023-06-01T08:00:00+08:00",
                },
            ]
        ).to_csv(source, index=False)
        import_history(
            source,
            root.parent / suffix,
            provider=provider,
            source_uri="vendor://" + suffix,
            license_note="test declaration",
        )
    recipe["inputs"]["history"] = str(root.parent / "one")
    summary = {
        "recipe": recipe,
        "definition_sha256": "history-study",
        "results": [
            {
                "candidate": spec["parameters"][0],
                "identity": spec["code_identity"],
                "status": "completed",
                "data_identity": paper.input_identity(recipe),
                "scope": spec["source_scope"],
                "metrics": spec["source_metrics"],
            }
        ],
    }
    monkeypatch.setattr(quant_report_hub.research_workbench, "load_study", lambda _: summary)
    target = root.parent / "history-account"
    paper.promote(
        root.parent / "study.json",
        "base",
        target,
        account_id="history-forward",
        start="2023-06-01",
        end="2023-06-05",
        now=datetime(2023, 5, 31, tzinfo=timezone.utc),
    )
    changed = {**recipe["inputs"], "history": str(root.parent / "two")}
    assert paper.input_prefix(changed, "2023-05-30") == paper.input_prefix(
        recipe["inputs"], "2023-05-30"
    )
    with pytest.raises(ValueError, match="lineage"):
        paper.observe(
            target,
            changed,
            as_of="2023-06-01",
            now=datetime(2023, 6, 1, 10, tzinfo=timezone.utc),
            executor=ledger,
        )


def test_promotion_cannot_rebaseline_inputs_revised_since_source_study(account, monkeypatch):
    root, _ = account
    monkeypatch.setattr(paper, "input_identity", lambda _: {"new": "rewritten snapshot"})
    with pytest.raises(ValueError, match="before promotion"):
        paper.promote(
            root.parent / "study.json",
            "base",
            root.parent / "changed-account",
            account_id="changed-account",
            start="2023-06-01",
            end="2023-06-05",
            now=datetime(2023, 5, 31, tzinfo=timezone.utc),
        )


def test_observation_recovers_derived_file_failure_without_a_second_terminal_event(
    account, monkeypatch
):
    root, inputs = account
    original = paper.atomic_json

    def fail_latest(path, value):
        if path.name == "latest.json":
            raise OSError("latest file temporarily unavailable")
        return original(path, value)

    monkeypatch.setattr(paper, "atomic_json", fail_latest)
    with pytest.raises(OSError, match="latest file"):
        paper.observe(
            root,
            inputs,
            as_of="2023-06-01",
            now=datetime(2023, 6, 1, 10, tzinfo=timezone.utc),
            executor=ledger,
        )
    assert [e["status"] for e in paper.TrialRegistry(root / "account.db").history("forward-a")] == [
        "running",
        "completed",
    ]
    monkeypatch.setattr(paper, "atomic_json", original)

    def unexpected_executor(*args):
        raise AssertionError("Committed observation must not rerun")

    restored = paper.observe(
        root,
        inputs,
        as_of="2023-06-01",
        now=datetime(2023, 6, 1, 10, tzinfo=timezone.utc),
        executor=unexpected_executor,
    )
    assert restored["as_of"] == "2023-06-01"
    assert (root / "latest.json").exists()
    assert "2023-06-01" in (root / "account.html").read_text(encoding="utf-8")


def test_promotion_requires_all_source_candidates_to_complete(account, monkeypatch):
    import quant_report_hub.research_workbench

    root, _ = account
    original = quant_report_hub.research_workbench.load_study
    summary = original(root.parent / "study.json")
    summary["results"].append(
        {"candidate": {"name": "failed-control"}, "status": "failed", "error": "missing data"}
    )
    with pytest.raises(ValueError, match="entire preregistered"):
        paper.promote(
            root.parent / "study.json",
            "base",
            root.parent / "partial-account",
            account_id="partial-account",
            start="2023-06-01",
            end="2023-06-05",
            now=datetime(2023, 5, 31, tzinfo=timezone.utc),
        )


def test_market_date_is_independent_of_timezone_representation(account):
    root, inputs = account
    paper.observe(
        root,
        inputs,
        as_of="2023-06-05",
        now=datetime(2023, 6, 5, 10, tzinfo=timezone.utc),
        executor=ledger,
    )
    instant = datetime(2023, 6, 5, 12, tzinfo=timezone.utc)
    for now in (instant, instant.astimezone(timezone(timedelta(hours=14)))):
        with pytest.raises(ValueError, match="not ended"):
            paper.seal(root, now=now)
    assert paper._now(instant) == paper._now(instant.astimezone(timezone(timedelta(hours=14))))


def test_weekend_start_and_stale_source_are_rejected_before_account_creation(account, monkeypatch):
    import a_share_multifactor.decision_workflow

    root, _ = account
    target = root.parent / "invalid-forward"
    with pytest.raises(ValueError, match="known exchange session"):
        paper.promote(
            root.parent / "study.json",
            "base",
            target,
            account_id="invalid-forward",
            start="2023-06-03",
            end="2023-06-05",
            now=datetime(2023, 5, 31, tzinfo=timezone.utc),
        )
    assert not target.exists()

    original = a_share_multifactor.decision_workflow.load_inputs

    def stale(path):
        manifest, frames = original(path)
        for key in ("raw", "adjusted"):
            frames[key] = frames[key][pd.to_datetime(frames[key].date) < "2023-05-30"]
        return manifest, frames

    monkeypatch.setattr(a_share_multifactor.decision_workflow, "load_inputs", stale)
    with pytest.raises(ValueError, match="snapshot is stale"):
        paper.promote(
            root.parent / "study.json",
            "base",
            target,
            account_id="invalid-forward",
            start="2023-06-01",
            end="2023-06-05",
            now=datetime(2023, 5, 31, tzinfo=timezone.utc),
        )
    assert not target.exists()


def test_weekend_end_is_a_calendar_boundary_and_seals_after_last_session(account):
    root, inputs = account
    target = root.parent / "weekend-end"
    paper.promote(
        root.parent / "study.json",
        "base",
        target,
        account_id="weekend-end",
        start="2023-06-01",
        end="2023-06-04",
        now=datetime(2023, 5, 31, tzinfo=timezone.utc),
    )
    paper.observe(
        target,
        inputs,
        as_of="2023-06-02",
        now=datetime(2023, 6, 2, 10, tzinfo=timezone.utc),
        executor=ledger,
    )
    evidence = paper.seal(target, now=datetime(2023, 6, 5, tzinfo=timezone.utc))
    assert evidence["end"] == "2023-06-04"
    assert evidence["strategy_net"]["sessions"] == 2


@pytest.mark.parametrize("failed_target", ["account", "report"])
def test_promotion_recovers_committed_registration_and_rejects_different_request(
    account, monkeypatch, failed_target
):
    root, _ = account
    target = root.parent / ("partial-" + failed_target)
    original_json, original_report = paper.atomic_json, paper.write_account_report

    def failing(*args):
        raise OSError("interrupted account publication")

    monkeypatch.setattr(
        paper, "atomic_json" if failed_target == "account" else "write_account_report", failing
    )
    kwargs = {"account_id": "partial-forward", "start": "2023-06-01", "end": "2023-06-05"}
    with pytest.raises(OSError, match="publication"):
        paper.promote(
            root.parent / "study.json",
            "base",
            target,
            **kwargs,
            now=datetime(2023, 5, 31, tzinfo=timezone.utc),
        )
    registered = paper.TrialRegistry(target / "account.db").definition("partial-forward")
    monkeypatch.setattr(paper, "atomic_json", original_json)
    monkeypatch.setattr(paper, "write_account_report", original_report)
    monkeypatch.setattr(paper, "equity_code_identity", lambda: {"code": "new-release"})
    monkeypatch.setattr(paper, "input_identity", lambda _: {"data": "newer-tail"})
    # Recovery uses the original registration time even if the first session has begun.
    recovered = paper.promote(
        root.parent / "study.json",
        "base",
        target,
        **kwargs,
        now=datetime(2023, 6, 1, 10, tzinfo=timezone.utc),
    )
    assert recovered["created_at"] == registered["registered_at"]
    assert (target / "account.html").exists()
    assert paper.load_account(target) == recovered
    original_prefix = paper.input_prefix
    monkeypatch.setattr(paper, "input_prefix", lambda *_: {"data": "revised-before-cutoff"})
    with pytest.raises(ValueError, match="inputs changed"):
        paper.promote(
            root.parent / "study.json",
            "base",
            target,
            **kwargs,
            now=datetime(2023, 6, 1, 10, tzinfo=timezone.utc),
        )
    monkeypatch.setattr(paper, "input_prefix", original_prefix)
    with paper.TrialRegistry(target / "account.db").connect() as db:
        assert db.execute("SELECT COUNT(*) FROM studies").fetchone()[0] == 1
    with pytest.raises(ValueError, match="differs"):
        paper.promote(
            root.parent / "study.json",
            "base",
            target,
            **{**kwargs, "end": "2023-06-06"},
            now=datetime(2023, 5, 31, tzinfo=timezone.utc),
        )
