"""Optional bounded evening acquisition policy; legacy profiles retain their slots."""

from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

ZONE = ZoneInfo("Asia/Shanghai")
LEGACY_SLOTS = (time(16, 30), time(17), time(18))


def validate(value):
    if value is None:
        return
    if not isinstance(value, dict) or set(value) != {
        "timezone",
        "slots",
        "collection_timeout_seconds",
    }:
        raise ValueError("Invalid ETF schedule fields")
    if value["timezone"] != "Asia/Shanghai":
        raise ValueError("ETF schedule must use Asia/Shanghai")
    slots = value["slots"]
    if not isinstance(slots, list) or not slots:
        raise ValueError("ETF schedule requires slots")
    try:
        parsed = [time.fromisoformat(s) for s in slots]
    except (ValueError, TypeError) as exc:
        raise ValueError("ETF slots must be HH:MM") from exc
    if (
        slots != [s.strftime("%H:%M") for s in parsed]
        or parsed != sorted(set(parsed))
        or parsed[0] < time(15)
        or parsed[-1] > time(23, 40)
    ):
        raise ValueError("ETF slots must be unique ordered post-close times")
    timeout = value["collection_timeout_seconds"]
    if type(timeout) is not int or not 1 <= timeout <= 600:
        raise ValueError("ETF collection timeout must be 1..600 seconds")


def window(profile, now, trading, history, as_of):
    config = profile.get("schedule")
    slots = [time.fromisoformat(s) for s in config["slots"]] if config else LEGACY_SLOTS
    local = now.astimezone(ZONE)
    starts = [datetime.combine(local.date(), s, ZONE) for s in slots]
    slot = max((i for i, start in enumerate(starts) if local >= start), default=-1)
    timeout = config["collection_timeout_seconds"] if config else None
    today = [r for r in history if r["as_of"] == as_of]
    last = today[-1] if today else None
    state = "ready"
    if not trading:
        state = "market_closed"
    elif slot < 0:
        state = "not_due"
    elif config and local >= starts[slot] + timedelta(seconds=timeout):
        state = "collection_exhausted" if slot == len(slots) - 1 else "waiting_for_slot"
    if last:
        if last["state"] == "completed":
            state = "completed"
        elif last["state"] == "research_ready":
            state = "research_ready"
        elif last["state"] not in {"collection_retryable", "collection_exhausted"}:
            state = "needs_attention"
        elif last["state"] == "collection_exhausted":
            state = "collection_exhausted"
        elif last["state"] == "collection_retryable" and state == "ready":
            state = "collection_retryable"
    retryable = not last or last["state"] == "collection_retryable"
    next_at = next((s.isoformat() for s in starts if s > local), None) if retryable else None
    if not trading:
        next_at = None
    return {
        "timezone": "Asia/Shanghai",
        "slots": [s.strftime("%H:%M") for s in slots],
        "slot": slot,
        "state": state,
        "next_attempt_at": next_at,
        "collection_timeout_seconds": timeout,
        "final_attempt_at": starts[-1].isoformat(),
        "last_slot": slot == len(slots) - 1,
    }
