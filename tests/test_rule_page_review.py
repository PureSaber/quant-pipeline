import hashlib

import pytest

from quant_pipeline.rule_page_review import reviewed_template_change


def test_only_exact_reviewed_template_edit_may_continue(tmp_path):
    old = b'<script src="ui-v1.js"></script><a href="rule.pdf">rule</a>'
    new = old.replace(b"ui-v1.js", b"ui-v2.js")
    (tmp_path / "original.html").write_bytes(old)
    sha = lambda v: hashlib.sha256(v).hexdigest()
    review = {
        "original_file": "original.html",
        "original_sha256": sha(old),
        "expected_sha256": sha(new),
        "replacements": [{"before": "ui-v1.js", "after": "ui-v2.js"}],
        "reviewed_at": "2026-10-22T01:00:00Z",
        "reason": "reviewed UI script version; rules unchanged",
        "content_scope": "complete-static-rule-directory",
    }
    assert reviewed_template_change(new, review, root=tmp_path)["classification"] == "template_only"
    changed = new.replace(b"rule.pdf", b"replacement.pdf")
    with pytest.raises(ValueError):
        reviewed_template_change(
            changed, {**review, "expected_sha256": sha(changed)}, root=tmp_path
        )
    with pytest.raises(ValueError):
        reviewed_template_change(new, {**review, "original_file": "../elsewhere"}, root=tmp_path)
    changed_review = {
        **review,
        "expected_sha256": sha(changed),
        "replacements": [
            *review["replacements"],
            {"before": "rule.pdf", "after": "replacement.pdf"},
        ],
    }
    with pytest.raises(ValueError, match="links or text"):
        reviewed_template_change(changed, changed_review, root=tmp_path)
    with pytest.raises(ValueError, match="coverage"):
        reviewed_template_change(new, {**review, "content_scope": "api-paginated"}, root=tmp_path)
