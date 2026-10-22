"""Exact, reviewed template edits. No wildcard removal of scripts or links."""

import hashlib
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path


class _StaticDirectory(HTMLParser):
    def __init__(self):
        super().__init__()
        self.links, self.text, self.hidden = [], [], 0

    def handle_starttag(self, tag, attrs):
        if tag in {"iframe", "frame", "object", "embed"}:
            raise ValueError(
                "embedded rule content requires a complete extractor; template scope unknown"
            )
        if tag in {"script", "style"}:
            self.hidden += 1
        if tag == "a":
            self.links.append(dict(attrs).get("href", ""))

    def handle_endtag(self, tag):
        if tag in {"script", "style"}:
            self.hidden = max(0, self.hidden - 1)

    def handle_data(self, data):
        if not self.hidden:
            self.text.extend(data.split())


def _content_identity(data):
    parser = _StaticDirectory()
    parser.feed(data.decode("utf-8"))
    if not parser.links:
        raise ValueError("static rule-directory extraction is empty")
    return parser.links, parser.text


def reviewed_template_change(content, review, *, root):
    fields = {
        "original_file",
        "original_sha256",
        "expected_sha256",
        "replacements",
        "reviewed_at",
        "reason",
        "content_scope",
    }
    if not isinstance(review, dict) or set(review) != fields or not review["reason"].strip():
        raise ValueError("template review requires an exact versioned review contract")
    stamp = datetime.fromisoformat(review["reviewed_at"].replace("Z", "+00:00"))
    if stamp.tzinfo is None or stamp > datetime.now(timezone.utc):
        raise ValueError("template review requires an actual past timezone-aware time")
    if review["content_scope"] != "complete-static-rule-directory":
        raise ValueError(
            "dynamic or incomplete rule coverage cannot be classified as template-only"
        )
    path = (Path(root) / review["original_file"]).resolve()
    if Path(root).resolve() not in path.parents:
        raise ValueError("template source escapes the review directory")
    original = path.read_bytes()
    sha = lambda data: hashlib.sha256(data).hexdigest()
    if sha(original) != review["original_sha256"] or sha(content) != review["expected_sha256"]:
        raise ValueError("template review identities do not match original/current bytes")
    replacements = review["replacements"]
    if not isinstance(replacements, list) or not 1 <= len(replacements) <= 8:
        raise ValueError("template review requires bounded literal replacements")
    transformed = original
    for item in replacements:
        if set(item) != {"before", "after"} or any(
            not isinstance(v, str) or not v or len(v) > 1024 for v in item.values()
        ):
            raise ValueError("template edits must be small explicit nonempty byte ranges")
        before, after = item["before"].encode(), item["after"].encode()
        if transformed.count(before) != 1:
            raise ValueError("template edit locator must match exactly once")
        transformed = transformed.replace(before, after, 1)
    if transformed != content:
        raise ValueError("unreviewed rule page changes remain outside template edits")
    if _content_identity(original) != _content_identity(content):
        raise ValueError("rule-directory links or text changed; typed rule review required")
    return {
        "classification": "template_only",
        "original_sha256": sha(original),
        "sha256": sha(content),
        "reviewed_at": review["reviewed_at"],
        "reason": review["reason"],
        "scope": "exact reviewed byte edits only; all attachments still verified independently",
    }
