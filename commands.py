from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import Optional

import discord
from discord import app_commands

import pytz

from .challenge_manager import ChallengeManager
from .timezones import normalize_timezone

LOGGER = logging.getLogger(__name__)


def _as_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except Exception as e:
        raise RuntimeError("Date must be YYYY-MM-DD") from e


def register_command_groups(bot: discord.Client, manager: ChallengeManager, app_config) -> None:
    tree = bot.tree

    # ---------------- /join ----------------
    @tree.command(name="join", description="Join the daily challenge")
    @app_commands.describe(gender="male or female", is_disabled="true if you need chair/floor-friendly punishments", timezone="IANA tz like America/Los_Angeles (or PST/EST etc)")
    async def join_cmd(
        interaction: discord.Interaction,
        gender: str,
        is_disabled: bool = False,
        timezone: str = "America/Los_Angeles",
    ) -> None:
        try:
            tz = normalize_timezone(timezone, default=app_config.challenge.default_timezone)
            p = manager.add_participant(
                discord_user=interaction.user,
                gender=gender,
                is_disabled=is_disabled,
                timezone=tz,
            )
            await interaction.response.send_message(
                f"✅ Joined! Saved timezone **{p.timezone}**.\n"
                "Next: set your challenge(s) with **/challenge add** (or just start logging with /log).",
                ephemeral=True,
            )
        except Exception as e:
            await interaction.response.send_message(f"❌ {e}", ephemeral=True)

    # ---------------- /log ----------------
    @tree.command(name="log", description="Log progress for today (or a specific day)")
    @app_commands.describe(
        amount="How many reps/seconds/steps you did",
        challenge_id="Which challenge to log (optional if you set a default)",
        log_date="YYYY-MM-DD (optional; defaults to today in YOUR timezone)",
        workout_bonus="Optional bonus amount",
        notes="Optional note",
    )
    async def log_cmd(
        interaction: discord.Interaction,
        amount: int,
        challenge_id: Optional[str] = None,
        log_date: Optional[str] = None,
        workout_bonus: Optional[int] = None,
        notes: Optional[str] = None,
    ) -> None:
        try:
            p = manager.get_participant(str(interaction.user.id))
            if not p:
                await interaction.response.send_message("❌ You’re not in the challenge yet. Use **/join** first.", ephemeral=True)
                return

            tz_name = normalize_timezone(p.timezone, default=app_config.challenge.default_timezone)
            tz = pytz.timezone(tz_name)

            if log_date:
                d = _as_date(log_date)
            else:
                d = datetime.now(tz).date()

            cid = (challenge_id or "").strip() or manager.resolve_default_challenge_id(p)
            # If they still have no challenge id, allow a legacy log (pushups) so the bot stays usable
            if not cid:
                cid = None

            manager.record_amount(
                participant_id=p.discord_id,
                log_date=d,
                amount=int(amount),
                challenge_id=cid,
                workout_bonus=workout_bonus,
                notes=notes,
            )

            await interaction.response.send_message(
                f"✅ Logged **{amount}** for **{d.isoformat()}**"
                + (f" (challenge: `{cid}`)" if cid else " (legacy log)"),
                ephemeral=True,
            )
        except Exception as e:
            await interaction.response.send_message(f"❌ {e}", ephemeral=True)

    # ---------------- /challenge (group) ----------------
    challenge_group = app_commands.Group(name="challenge", description="Manage your daily challenge(s)")

    @challenge_group.command(name="add", description="Add a new daily challenge for yourself")
    @app_commands.describe(challenge_type="pushups, squats, plank, steps, or any custom label", daily_target="Target number", unit="reps/seconds/minutes/steps", set_default="Set as your default for /log")
    async def challenge_add(
        interaction: discord.Interaction,
        challenge_type: str,
        daily_target: int,
        unit: str = "reps",
        set_default: bool = False,
    ) -> None:
        try:
            p = manager.get_participant(str(interaction.user.id))
            if not p:
                await interaction.response.send_message("❌ Use **/join** first.", ephemeral=True)
                return
            ch = manager.add_challenge(
                discord_id=p.discord_id,
                challenge_type=challenge_type,
                daily_target=daily_target,
                unit=unit,
                set_default=set_default,
            )
            await interaction.response.send_message(
                f"✅ Added challenge: **{ch.challenge_type}** — target **{ch.daily_target} {ch.unit}**\n"
                f"ID: `{ch.challenge_id}`" + (" (default)" if set_default else ""),
                ephemeral=True,
            )
        except Exception as e:
            await interaction.response.send_message(f"❌ {e}", ephemeral=True)

    @challenge_group.command(name="list", description="List your active challenges")
    async def challenge_list(interaction: discord.Interaction) -> None:
        try:
            p = manager.get_participant(str(interaction.user.id))
            if not p:
                await interaction.response.send_message("❌ Use **/join** first.", ephemeral=True)
                return
            items = manager.list_challenges(p.discord_id, active_only=True)
            if not items:
                await interaction.response.send_message("You have no active challenges yet. Add one with **/challenge add**.", ephemeral=True)
                return

            default_id = manager.resolve_default_challenge_id(p)
            lines = []
            for c in items:
                tag = " ⭐ default" if default_id and c.challenge_id == default_id else ""
                lines.append(f"• `{c.challenge_id}` — **{c.challenge_type}**: {c.daily_target} {c.unit}{tag}")

            await interaction.response.send_message("\n".join(lines), ephemeral=True)
        except Exception as e:
            await interaction.response.send_message(f"❌ {e}", ephemeral=True)

    @challenge_group.command(name="remove", description="Deactivate a challenge")
    @app_commands.describe(challenge_id="ID from /challenge list")
    async def challenge_remove(interaction: discord.Interaction, challenge_id: str) -> None:
        try:
            p = manager.get_participant(str(interaction.user.id))
            if not p:
                await interaction.response.send_message("❌ Use **/join** first.", ephemeral=True)
                return
            ok = manager.remove_challenge(discord_id=p.discord_id, challenge_id=challenge_id)
            await interaction.response.send_message("✅ Removed." if ok else "❌ Could not remove.", ephemeral=True)
        except Exception as e:
            await interaction.response.send_message(f"❌ {e}", ephemeral=True)

    @challenge_group.command(name="setdefault", description="Set your default challenge for /log")
    @app_commands.describe(challenge_id="ID from /challenge list (leave empty to clear)")
    async def challenge_setdefault(interaction: discord.Interaction, challenge_id: str) -> None:
        try:
            p = manager.get_participant(str(interaction.user.id))
            if not p:
                await interaction.response.send_message("❌ Use **/join** first.", ephemeral=True)
                return
            manager.set_default_challenge(discord_id=p.discord_id, challenge_id=challenge_id)
            await interaction.response.send_message(f"✅ Default challenge set to `{challenge_id}`.", ephemeral=True)
        except Exception as e:
            await interaction.response.send_message(f"❌ {e}", ephemeral=True)

    # ---------------- /admin (group) ----------------
    admin_group = app_commands.Group(name="admin", description="Admin controls (requires Manage Server)")

    def _is_admin(interaction: discord.Interaction) -> bool:
        if not interaction.user or not isinstance(interaction.user, discord.Member):
            return False
        return interaction.user.guild_permissions.manage_guild

    @admin_group.command(name="set_mode", description="Set compliance mode: strict | lenient | points")
    @app_commands.describe(mode="strict, lenient, or points")
    async def admin_set_mode(interaction: discord.Interaction, mode: str) -> None:
        if not _is_admin(interaction):
            await interaction.response.send_message("❌ You need **Manage Server** to run this.", ephemeral=True)
            return
        try:
            m = manager.set_compliance_mode(mode)
            await interaction.response.send_message(f"✅ Compliance mode set to **{m}**.", ephemeral=True)
        except Exception as e:
            await interaction.response.send_message(f"❌ {e}", ephemeral=True)

    @admin_group.command(name="set_points_target", description="In points mode, set how many challenges must be completed per day")
    @app_commands.describe(points="Minimum points per day (>=1)")
    async def admin_set_points(interaction: discord.Interaction, points: int) -> None:
        if not _is_admin(interaction):
            await interaction.response.send_message("❌ You need **Manage Server** to run this.", ephemeral=True)
            return
        try:
            t = manager.set_points_target(points)
            await interaction.response.send_message(f"✅ Points target set to **{t}**.", ephemeral=True)
        except Exception as e:
            await interaction.response.send_message(f"❌ {e}", ephemeral=True)

    @admin_group.command(name="mode", description="Show current compliance mode settings")
    async def admin_mode(interaction: discord.Interaction) -> None:
        try:
            mode = manager.compliance_mode()
            pts = manager.points_target()
            await interaction.response.send_message(
                f"Mode: **{mode}**\nPoints target (only matters in points mode): **{pts}**",
                ephemeral=True,
            )
        except Exception as e:
            await interaction.response.send_message(f"❌ {e}", ephemeral=True)

    # ---------------- /status ----------------
    @tree.command(name="status", description="Show your status for today (in your timezone)")
    async def status_cmd(interaction: discord.Interaction) -> None:
        try:
            p = manager.get_participant(str(interaction.user.id))
            if not p:
                await interaction.response.send_message("❌ Use **/join** first.", ephemeral=True)
                return

            tz_name = normalize_timezone(p.timezone, default=app_config.challenge.default_timezone)
            tz = pytz.timezone(tz_name)
            today = datetime.now(tz).date()

            st = manager.evaluate_multi_compliance(today).get(p.discord_id)
            if not st:
                await interaction.response.send_message("❌ Couldn't compute status right now.", ephemeral=True)
                return

            if st.get("mode") == "legacy":
                met = (st.get("met") or [{}])[0]
                msg = (
                    f"Today: **{today.isoformat()}**\n"
                    f"Done: **{met.get('done')}** / Target: **{met.get('target')} reps**\n"
                    f"Compliant: **{st.get('compliant')}**"
                )
                await interaction.response.send_message(msg, ephemeral=True)
                return

            mode = st.get("mode")
            points = st.get("points")
            target = st.get("points_target")
            missing = st.get("missing") or []
            miss_lines = []
            for m in missing[:5]:
                miss_lines.append(f"• {m.get('type')} — need {m.get('need')} {m.get('unit')} (`{m.get('challenge_id')}`)")
            miss_text = "\n".join(miss_lines) if miss_lines else "None 🎉"

            await interaction.response.send_message(
                f"Today: **{today.isoformat()}**\n"
                f"Mode: **{mode}**\n"
                f"Progress: **{points} / {target}**\n"
                f"Compliant: **{st.get('compliant')}**\n"
                f"Missing:\n{miss_text}",
                ephemeral=True,
            )
        except Exception as e:
            await interaction.response.send_message(f"❌ {e}", ephemeral=True)

    # ---------------- /dayoff (simple) ----------------
    dayoff_group = app_commands.Group(name="dayoff", description="Request or vote for a day off")

    @dayoff_group.command(name="request", description="Request a day off (vote-based)")
    @app_commands.describe(target_day="YYYY-MM-DD (in your timezone)", reason="Optional reason")
    async def dayoff_request(interaction: discord.Interaction, target_day: str, reason: Optional[str] = None) -> None:
        try:
            p = manager.get_participant(str(interaction.user.id))
            if not p:
                await interaction.response.send_message("❌ Use **/join** first.", ephemeral=True)
                return
            d = _as_date(target_day)
            deadline = datetime.utcnow().replace(tzinfo=pytz.UTC) + timedelta(hours=12)
            req = manager.create_day_off_request(
                requested_by=p.discord_id,
                target_day=d,
                reason=reason,
                deadline=deadline,
            )
            await interaction.response.send_message(
                f"✅ Day-off request created for **{d.isoformat()}**.\n"
                f"Request ID: `{req.request_id}`\n"
                "Ask participants to vote with **/dayoff vote**.",
                ephemeral=True,
            )
        except Exception as e:
            await interaction.response.send_message(f"❌ {e}", ephemeral=True)

    @dayoff_group.command(name="vote", description="Vote on a day-off request")
    @app_commands.describe(request_id="Request ID", vote="yes or no")
    async def dayoff_vote(interaction: discord.Interaction, request_id: str, vote: str) -> None:
        try:
            p = manager.get_participant(str(interaction.user.id))
            if not p:
                await interaction.response.send_message("❌ Use **/join** first.", ephemeral=True)
                return
            manager.register_vote(request_id=request_id, voter_id=p.discord_id, vote=vote)
            await interaction.response.send_message("✅ Vote recorded.", ephemeral=True)
        except Exception as e:
            await interaction.response.send_message(f"❌ {e}", ephemeral=True)

    @dayoff_group.command(name="status", description="Check vote status for a request")
    @app_commands.describe(request_id="Request ID")
    async def dayoff_status(interaction: discord.Interaction, request_id: str) -> None:
        try:
            s = manager.compute_vote_state(request_id)
            await interaction.response.send_message(
                f"Request `{request_id}` — state: **{s['state']}** (yes {s['yes']} / no {s['no']} / total {s['total']}, threshold {s['threshold']})",
                ephemeral=True,
            )
        except Exception as e:
            await interaction.response.send_message(f"❌ {e}", ephemeral=True)

    tree.add_command(challenge_group)
    tree.add_command(admin_group)
    tree.add_command(dayoff_group)
