"""Refresh unchanged official evidence, with fail-closed rule-directory guards.

This does not interpret new rules. Any changed attachment or directory requires
a new reviewed source declaration. Validity ends at actual acquisition time.
"""

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
from quant_data_kit.failure_policy import EvidenceFault
from quant_lab.research import file_hash


def fetch(url, *, form=None):
    import requests

    def official(value):
        parsed = urlparse(value)
        return (
            parsed.scheme == "https"
            and parsed.hostname
            in {
                "www.sse.com.cn",
                "www.szse.cn",
                "docs.static.szse.cn",
                "disc.static.szse.cn",
                "reportdocs.static.szse.cn",
            }
            and not parsed.username
            and parsed.port in {None, 443}
        )

    if not official(url):
        raise ValueError("Master evidence requires an official HTTPS source")
    # No automatic redirects to an unreviewed host or replacement resource.
    response = requests.request(
        "POST" if form is not None else "GET",
        url,
        data=form,
        timeout=(10, 45),
        allow_redirects=False,
    )
    response.raise_for_status()
    if response.status_code != 200 or not 0 < len(response.content) <= 20_000_000:
        raise ValueError("Official evidence response is empty, redirected or too large")
    return response.content


def refresh(config_path, directory):
    from quant_data_kit.instrument_master import CURRENT_VALIDITY, import_instrument_master

    config_path, directory = Path(config_path), Path(directory)
    config = json.loads(config_path.read_text(encoding="utf-8"))
    if set(config) != {"schema", "source", "source_sha256", "guards"} or config["schema"] not in {
        "quant.etf-master-refresh/v1",
        "quant.etf-master-refresh/v2",
    }:
        raise ValueError("Master refresh configuration does not match v1")
    source = Path(config["source"])
    if (
        not source.is_absolute()
        or file_hash(source / "declaration.json") != config["source_sha256"]
    ):
        raise EvidenceFault("identity_changed", "Reviewed master source identity changed")
    guards = config["guards"]
    if not isinstance(guards, list) or len(guards) < 2:
        raise ValueError("Current exchange rule-directory guards are required")
    directory.mkdir(exist_ok=False)
    declaration = json.loads((source / "declaration.json").read_text(encoding="utf-8"))
    evidence = []
    for index, guard in enumerate(guards):
        allowed = {"url", "form", "sha256"}
        if config["schema"] == "quant.etf-master-refresh/v2" and "template_review" in guard:
            allowed.add("template_review")
        if set(guard) != allowed:
            raise ValueError("Invalid official rule-directory guard")
        content = fetch(guard["url"], form=guard["form"])
        digest = hashlib.sha256(content).hexdigest()
        (directory / f"guard-{index}.bin").write_bytes(content)
        if digest != guard["sha256"]:
            review = guard.get("template_review")
            if not review or review.get("original_sha256") != guard["sha256"]:
                raise EvidenceFault(
                    "unparsed", "Official rule directory changed; review before extending evidence"
                )
            from quant_pipeline.rule_page_review import reviewed_template_change

            try:
                assessment = reviewed_template_change(content, review, root=config_path.parent)
            except ValueError as exc:
                raise EvidenceFault("unparsed", str(exc)) from exc
            evidence.append({"url": guard["url"], **assessment})
            continue
        evidence.append({"url": guard["url"], "sha256": digest})
    copied = directory / "source"
    copied.mkdir()
    for document in declaration["documents"]:
        relative = Path(document["file"])
        target = (copied / relative).resolve()
        original = (source / relative).resolve()
        if copied.resolve() not in target.parents or source.resolve() not in original.parents:
            raise ValueError("Master attachment escapes source directory")
        if file_hash(original) != document["sha256"]:
            raise EvidenceFault("identity_changed", "Reviewed master attachment changed")
        content = fetch(document["source_uri"])
        digest = hashlib.sha256(content).hexdigest()
        if digest != document["sha256"]:
            raise EvidenceFault(
                "rule_changed", "Official master attachment changed; new typed assertions required"
            )
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)
        evidence.append({"url": document["source_uri"], "sha256": digest})
    # Capture after the last successful fetch, never a caller-supplied future date.
    captured = datetime.now(timezone.utc).isoformat()
    previous_capture = declaration["captured_at"]
    if pd.Timestamp(captured) <= pd.Timestamp(previous_capture):
        raise ValueError("Master refresh time must follow prior capture")
    catalog_path = (source / declaration["catalog_file"]).resolve()
    if (
        source.resolve() not in catalog_path.parents
        or file_hash(catalog_path) != declaration["catalog_sha256"]
    ):
        raise EvidenceFault("identity_changed", "Reviewed catalog changed")
    catalog = pd.read_csv(catalog_path, dtype=str, keep_default_na=False)
    current = pd.to_datetime(catalog.effective_to, utc=True, format="mixed").eq(
        pd.Timestamp(previous_capture)
    )
    catalog.loc[current, "effective_to"] = captured
    for document in declaration["documents"]:
        if document["validity_basis"] == CURRENT_VALIDITY:
            document["effective_to"] = captured
    declaration["captured_at"] = captured
    declaration["catalog_file"] = "catalog.csv"
    catalog.to_csv(copied / "catalog.csv", index=False)
    declaration["catalog_sha256"] = file_hash(copied / "catalog.csv")
    (copied / "declaration.json").write_text(
        json.dumps(declaration, ensure_ascii=False), encoding="utf-8"
    )
    # Native import remains responsible for schema, typed fields, PIT and hashes.
    bundle = directory / "bundle"
    manifest = import_instrument_master(copied, bundle)
    receipt = {
        "captured_at": captured,
        "config_sha256": file_hash(config_path),
        "evidence": evidence,
        "bundle_sha256": manifest["bundle_sha256"],
    }
    (directory / "refresh.json").write_text(
        json.dumps(receipt, ensure_ascii=False), encoding="utf-8"
    )
    return bundle, receipt
