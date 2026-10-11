"""JSON command-line client; closing this process does not stop workers."""

import argparse
import json
from pathlib import Path

from .durable_jobs import JobStore, canonical


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True, type=Path)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("capabilities")
    submit = commands.add_parser("submit")
    submit.add_argument("--request", required=True, type=Path)
    submit.add_argument("--key", required=True)
    submit.add_argument("--research", action="store_true", help="retain QDK inputs before submit")
    for action in ("get", "cancel", "events", "attempts"):
        commands.add_parser(action).add_argument("job_id")
    listing = commands.add_parser("list")
    listing.add_argument("--status")
    listing.add_argument("--limit", type=int, default=100)
    batch_submit = commands.add_parser("batch-submit")
    batch_submit.add_argument("--request", required=True, type=Path)
    batch_submit.add_argument("--key", required=True)
    batch_submit.add_argument("--research", action="store_true")
    for action in ("batch-get", "batch-cancel"):
        commands.add_parser(action).add_argument("batch_id")
    batch_list = commands.add_parser("batch-list")
    batch_list.add_argument("--limit", type=int, default=100)
    batch_list.add_argument("--project-id")
    retry = commands.add_parser("batch-retry")
    retry.add_argument("batch_id")
    retry.add_argument("--key", required=True)
    retry.add_argument("--candidate", action="append", required=True)
    retry.add_argument("--research", action="store_true")
    args = parser.parse_args(argv)
    store = JobStore(args.database)
    if args.command == "capabilities":
        result = {
            "schema_version": "quant.durable-capabilities/v1",
            "batch_request": "quant.job-batch/v1",
        }
    elif args.command.startswith("batch-"):
        from .durable_batches import BatchStore

        batches = BatchStore(store)
        if args.command == "batch-submit":
            request = json.loads(args.request.read_text(encoding="utf-8"))
            if args.research:
                from .durable_research import submit_research_batch

                result = submit_research_batch(store, request, args.key)
            else:
                result = batches.submit(request, args.key)
        elif args.command == "batch-list":
            result = batches.list(limit=args.limit, project_id=args.project_id)
        elif args.command == "batch-retry":
            if args.research:
                from .durable_research import retry_research_batch

                result = retry_research_batch(store, args.batch_id, args.key, args.candidate)
            else:
                result = batches.retry(args.batch_id, args.key, args.candidate)
        else:
            result = getattr(batches, args.command.removeprefix("batch-"))(args.batch_id)
    elif args.command == "submit":
        request = json.loads(args.request.read_text(encoding="utf-8"))
        if args.research:
            from .durable_research import submit_research

            result = submit_research(store, request, args.key)
        else:
            result = store.submit(request, args.key)
    elif args.command == "list":
        result = store.list(status=args.status, limit=args.limit)
    else:
        result = getattr(store, args.command)(args.job_id)
    print(canonical(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
