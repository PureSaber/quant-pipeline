import json
from pathlib import Path

import pandas as pd
import pytest
from quant_lab.research import file_hash

from quant_pipeline import etf_master_refresh as refresh


def setup_source(tmp_path, monkeypatch):
    import quant_data_kit.instrument_master as master

    source = tmp_path / "reviewed"
    source.mkdir()
    (source / "official.txt").write_text("original official rule")
    captured = "2025-01-01T00:00:00Z"
    pd.DataFrame(
        [
            {"effective_to": "2024-01-01T00:00:00Z", "available_at": "2023-01-01T00:00:00Z"},
            {"effective_to": captured, "available_at": "2024-01-01T00:00:00Z"},
        ]
    ).to_csv(source / "catalog.csv", index=False)
    declaration = {
        "captured_at": captured,
        "catalog_file": "catalog.csv",
        "catalog_sha256": file_hash(source / "catalog.csv"),
        "documents": [
            {
                "file": "official.txt",
                "sha256": file_hash(source / "official.txt"),
                "source_uri": "https://www.sse.com.cn/official.txt",
                "validity_basis": master.CURRENT_VALIDITY,
                "effective_to": captured,
            }
        ],
    }
    (source / "declaration.json").write_text(json.dumps(declaration))
    guard = {
        "url": "https://www.sse.com.cn/rules",
        "form": None,
        "sha256": file_hash(source / "official.txt"),
    }
    config = tmp_path / "config.json"
    config.write_text(
        json.dumps(
            {
                "schema": "quant.etf-master-refresh/v1",
                "source": str(source),
                "source_sha256": file_hash(source / "declaration.json"),
                "guards": [guard, guard],
            }
        )
    )
    monkeypatch.setattr(refresh, "fetch", lambda *a, **k: (source / "official.txt").read_bytes())
    imported = []

    def native_import(path, output):
        imported.append(Path(path))
        output.mkdir()
        return {"bundle_sha256": "verified-by-native-import"}

    monkeypatch.setattr(master, "import_instrument_master", native_import)
    return config, source, imported


def test_refresh_extends_only_current_observation_and_keeps_original(tmp_path, monkeypatch):
    config, source, imported = setup_source(tmp_path, monkeypatch)
    original = file_hash(source / "declaration.json")
    bundle, receipt = refresh.refresh(config, tmp_path / "operation")
    copied = imported[0]
    declaration = json.loads((copied / "declaration.json").read_text())
    catalog = pd.read_csv(copied / "catalog.csv")
    assert catalog.iloc[0].effective_to == "2024-01-01T00:00:00Z"
    assert catalog.iloc[1].effective_to == receipt["captured_at"]
    assert declaration["documents"][0]["effective_to"] == receipt["captured_at"]
    assert catalog.iloc[1].available_at == "2024-01-01T00:00:00Z"
    assert file_hash(source / "declaration.json") == original
    assert bundle.is_dir() and len(receipt["evidence"]) == 3


@pytest.mark.parametrize("changed", ["directory", "document"])
def test_changed_official_evidence_never_publishes_bundle(tmp_path, monkeypatch, changed):
    config, source, imported = setup_source(tmp_path, monkeypatch)

    def fetch(url, **kwargs):
        return (
            b"changed"
            if (url.endswith("rules") == (changed == "directory"))
            else (source / "official.txt").read_bytes()
        )

    monkeypatch.setattr(refresh, "fetch", fetch)
    with pytest.raises(ValueError, match="changed"):
        refresh.refresh(config, tmp_path / "operation")
    assert not imported
    assert not (tmp_path / "operation/bundle").exists()


def test_refuses_nonofficial_transport_before_request():
    with pytest.raises(ValueError, match="official HTTPS"):
        refresh.fetch("http://www.sse.com.cn/rules")
