from __future__ import annotations

import re
from typing import Optional

import pytz


_ALIASES = {
    "pst": "America/Los_Angeles",
    "pdt": "America/Los_Angeles",
    "est": "America/New_York",
    "edt": "America/New_York",
    "cst": "America/Chicago",
    "cdt": "America/Chicago",
    "mst": "America/Denver",
    "mdt": "America/Denver",
    "gmt": "Etc/UTC",
    "utc": "Etc/UTC",
}


def normalize_timezone(value: Optional[str], *, default: str) -> str:
    """Return a pytz-valid IANA tz name (best-effort)."""
    v = (value or "").strip()
    if not v:
        return default

    v_low = v.lower()
    if v_low in _ALIASES:
        return _ALIASES[v_low]

    # Convert common "US/Pacific" etc if present in pytz
    if v in pytz.all_timezones:
        return v

    # Some users paste "America/Los_Angeles " with spaces
    v2 = re.sub(r"\s+", "", v)
    if v2 in pytz.all_timezones:
        return v2

    return default
