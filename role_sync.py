"""Utilities for syncing Discord roles based on challenge status."""

from __future__ import annotations

import logging
from typing import Optional, TYPE_CHECKING

import discord

from . import role_ids

if TYPE_CHECKING:
    from .challenge_manager import ChallengeManager

LOGGER = logging.getLogger(__name__)


async def sync_compliance_roles(
    member: discord.Member,
    is_compliant: bool,
) -> None:
    """Sync compliant/non-compliant roles for a member."""
    try:
        guild = member.guild
        compliant_role = guild.get_role(role_ids.ROLE_COMPLIANT)
        non_compliant_role = guild.get_role(role_ids.ROLE_NON_COMPLIANT)

        if is_compliant:
            # Add Compliant, remove Non-Compliant
            if compliant_role and compliant_role not in member.roles:
                await member.add_roles(compliant_role, reason="Met daily target")
            if non_compliant_role and non_compliant_role in member.roles:
                await member.remove_roles(non_compliant_role, reason="Met daily target")
        else:
            # Add Non-Compliant, remove Compliant
            if non_compliant_role and non_compliant_role not in member.roles:
                await member.add_roles(non_compliant_role, reason="Did not meet daily target")
            if compliant_role and compliant_role in member.roles:
                await member.remove_roles(compliant_role, reason="Did not meet daily target")

    except Exception as e:
        LOGGER.warning(f"Failed to sync compliance roles for {member}: {e}")


async def sync_all_compliance_roles(
    bot: discord.Client,
    manager: ChallengeManager,
    guild_id: Optional[int] = None,
) -> dict[str, int]:
    """
    Sync compliance roles for all participants in the challenge.

    Returns:
        Dict with counts: {"compliant_updated": N, "non_compliant_updated": N, "errors": N}
    """
    from datetime import date

    stats = {"compliant_updated": 0, "non_compliant_updated": 0, "errors": 0}

    if not guild_id:
        LOGGER.warning("No guild_id provided for role sync")
        return stats

    guild = bot.get_guild(guild_id)
    if not guild:
        LOGGER.warning(f"Guild {guild_id} not found")
        return stats

    # Get today's compliance status for all participants
    today = date.today()
    compliance_status = manager.evaluate_multi_compliance(today)

    for participant in manager.get_participants():
        try:
            member = guild.get_member(int(participant.discord_id))
            if not member:
                continue

            status = compliance_status.get(participant.discord_id, {})
            is_compliant = bool(status.get("compliant", False))

            await sync_compliance_roles(member, is_compliant)

            if is_compliant:
                stats["compliant_updated"] += 1
            else:
                stats["non_compliant_updated"] += 1

        except Exception as e:
            LOGGER.warning(f"Failed to sync roles for participant {participant.discord_id}: {e}")
            stats["errors"] += 1

    LOGGER.info(f"Role sync complete: {stats}")
    return stats
