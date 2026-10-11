"""JSON command-line client; closing this process does not stop workers."""

import argparse
import json
from pathlib import Path

from .durable_jobs import JobStore, canonical


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True, type=Path)
    commands = parser.add_subparsers(dest="command", required=True)
    submit = commands.add_parser("submit")
    submit.add_argument("--request", required=True, type=Path)
    submit.add_argument("--key", required=True)
    submit.add_argument("--research", action="store_true", help="retain QDK inputs before submit")
    for action in ("get", "cancel", "events", "attempts"):
        commands.add_parser(action).add_argument("job_id")
    listing = commands.add_parser("list")
    listing.add_argument("--status")
    listing.add_argument("--limit", type=int, default=100)
    args = parser.parse_args(argv)
    store = JobStore(args.database)
    if args.command == "submit":
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
