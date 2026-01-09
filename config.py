from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass(slots=True)
class BotConfig:
    token: str
    guild_id: Optional[int] = None

    # Channel IDs for auto-posting
    daily_checkins_channel_id: Optional[int] = None
    leaderboards_channel_id: Optional[int] = None
    motivation_channel_id: Optional[int] = None
    punishment_channel_id: Optional[int] = None


@dataclass(slots=True)
class SheetsConfig:
    spreadsheet_id: str
    credentials_path: Path


@dataclass(slots=True)
class ChallengeConfig:
    default_timezone: str = "America/Los_Angeles"

    # Daily targets (fallback when a user has no challenges configured yet)
    target_male: int = 200
    target_female: int = 100
    target_default: int = 200

    # Optional override for disabled participants (fallback mode only)
    disabled_daily_target: Optional[int] = None

    # Compliance modes for multi-challenge users:
    #   strict  -> must complete ALL active challenges
    #   lenient -> must complete ANY 1 active challenge
    #   points  -> earn 1 point per completed challenge; must reach points_daily_target
    compliance_mode_default: str = "strict"
    points_daily_target_default: int = 1

    # Messaging times (participant local time)
    motivation_time_local: str = "18:00"   # 6pm local
    reminder_time_local: str = "22:00"
    congrats_time_local: str = "20:00"     # (scheduler checks every minute anyway)

    # Channel posting times (server timezone - default_timezone)
    daily_checkin_time: str = "06:00"      # Morning check-in
    leaderboard_time: str = "20:00"        # Evening leaderboard update

    # Punishment at local midnight for yesterday
    punishment_run_time_local: str = "00:05"

    # Optional start date for the challenge (ignore punishments before this day)
    # Format: YYYY-MM-DD (or empty)
    start_date: Optional[str] = None


@dataclass(slots=True)
class AppConfig:
    bot: BotConfig
    sheets: SheetsConfig
    challenge: ChallengeConfig


def load_config() -> AppConfig:
    """Load config from environment variables (.env is fine)."""
    token = os.getenv("DISCORD_TOKEN", "").strip()
    if not token:
        raise RuntimeError("Missing DISCORD_TOKEN")

    guild_id_raw = os.getenv("GUILD_ID", "").strip()
    guild_id = int(guild_id_raw) if guild_id_raw else None

    spreadsheet_id = os.getenv("SHEET_ID", "").strip() or os.getenv("SPREADSHEET_ID", "").strip()
    if not spreadsheet_id:
        raise RuntimeError("Missing SHEET_ID (or SPREADSHEET_ID)")

    creds_path = (
        os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
        or os.getenv("CREDENTIALS_PATH", "").strip()
        or "/tmp/google-sa.json"  # Default for Docker deployment
    )
    if not creds_path:
        raise RuntimeError("Missing GOOGLE_APPLICATION_CREDENTIALS (service account json path)")

    default_tz = os.getenv("DEFAULT_TIMEZONE", "America/Los_Angeles").strip()

    def _int(name: str, default: int) -> int:
        v = os.getenv(name, "").strip()
        if not v:
            return default
        try:
            return int(v)
        except Exception:
            return default

    def _opt_int(name: str) -> Optional[int]:
        v = os.getenv(name, "").strip()
        if not v:
            return None
        try:
            return int(v)
        except Exception:
            return None

    compliance_mode = (os.getenv("COMPLIANCE_MODE", "strict").strip().lower() or "strict")
    if compliance_mode not in {"strict", "lenient", "points"}:
        compliance_mode = "strict"

    challenge = ChallengeConfig(
        default_timezone=default_tz,
        target_male=_int("TARGET_MALE", 200),
        target_female=_int("TARGET_FEMALE", 100),
        target_default=_int("TARGET_DEFAULT", 200),
        disabled_daily_target=_opt_int("DISABLED_DAILY_TARGET"),
        compliance_mode_default=compliance_mode,
        points_daily_target_default=_int("POINTS_DAILY_TARGET", 1),
        motivation_time_local=os.getenv("MOTIVATION_TIME_LOCAL", "18:00").strip(),
        reminder_time_local=os.getenv("REMINDER_TIME_LOCAL", "22:00").strip(),
        congrats_time_local=os.getenv("CONGRATS_TIME_LOCAL", "20:00").strip(),
        punishment_run_time_local=os.getenv("PUNISHMENT_TIME_LOCAL", "00:05").strip(),
        daily_checkin_time=os.getenv("DAILY_CHECKIN_TIME", "06:00").strip(),
        leaderboard_time=os.getenv("LEADERBOARD_TIME", "20:00").strip(),
        start_date=os.getenv("CHALLENGE_START_DATE", "").strip() or None,
    )

    return AppConfig(
        bot=BotConfig(
            token=token,
            guild_id=guild_id,
            daily_checkins_channel_id=_opt_int("DAILY_CHECKINS_CHANNEL_ID"),
            leaderboards_channel_id=_opt_int("LEADERBOARDS_CHANNEL_ID"),
            motivation_channel_id=_opt_int("MOTIVATION_CHANNEL_ID"),
            punishment_channel_id=_opt_int("PUNISHMENT_CHANNEL_ID"),
        ),
        sheets=SheetsConfig(spreadsheet_id=spreadsheet_id, credentials_path=Path(creds_path)),
        challenge=challenge,
    )
