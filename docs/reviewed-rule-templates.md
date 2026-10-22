# Reviewed rule-directory templates

Existing `quant.etf-master-refresh/v1` configurations retain strict raw-byte guards.
An explicitly new v2 configuration may add a `template_review` to a guard: exact
original file/hash, expected new hash, at most eight literal replacements,
past aware review time, reason, and `content_scope=complete-static-rule-directory`.

Both byte identities and every literal replacement must match. The static
extractor additionally requires unchanged link targets/order and visible text;
embedded content, empty extraction, dynamic/paginated coverage or any unexplained
change remains blocked. This restricted path is for reviewed complete static
directories; it is not a generic HTML/JS change classifier. Every rule attachment
is still independently fetched and hash checked. New rules require new typed
assertions and effective-time evidence. Typed failures propagate identity changes
as hard stops and unparsed/changed rules as account blocks.

No existing frozen account configuration is migrated by this code. A template
warning cannot itself manufacture native engineering or research acceptance credit.
