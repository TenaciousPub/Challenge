from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import Optional

import discord
from discord import app_commands

import pytz

from .challenge_manager import ChallengeManager
from .timezones import normalize_timezone
from . import role_ids
from .role_sync import sync_compliance_roles

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
        await interaction.response.defer(ephemeral=True)
        try:
            tz = normalize_timezone(timezone, default=app_config.challenge.default_timezone)
            p = manager.add_participant(
                discord_user=interaction.user,
                gender=gender,
                is_disabled=is_disabled,
                timezone=tz,
            )

            # Assign roles
            roles_assigned = []
            if interaction.guild and isinstance(interaction.user, discord.Member):
                try:
                    # Assign Challenge Participant role
                    participant_role = interaction.guild.get_role(role_ids.ROLE_CHALLENGE_PARTICIPANT)
                    if participant_role and participant_role not in interaction.user.roles:
                        await interaction.user.add_roles(participant_role, reason="Joined challenge")
                        roles_assigned.append(participant_role.name)

                    # Assign gender role
                    gender_role_id = role_ids.get_gender_role_id(gender)
                    if gender_role_id:
                        gender_role = interaction.guild.get_role(gender_role_id)
                        if gender_role and gender_role not in interaction.user.roles:
                            await interaction.user.add_roles(gender_role, reason=f"Gender: {gender}")
                            roles_assigned.append(gender_role.name)
                except Exception as e:
                    LOGGER.warning(f"Failed to assign roles to {interaction.user}: {e}")

            roles_msg = f"\n🎭 Roles assigned: {', '.join(roles_assigned)}" if roles_assigned else ""
            await interaction.followup.send(
                f"✅ Joined! Saved timezone **{p.timezone}**.{roles_msg}\n"
                "Next: set your challenge(s) with **/challenge add** (or just start logging with /log).",
                ephemeral=True,
            )
        except Exception as e:
            await interaction.followup.send(f"❌ {e}", ephemeral=True)

    # ---------------- /log ----------------
    # Helper function to resolve challenge ID or type
    def _resolve_challenge_reference(discord_id: str, reference: str) -> Optional[str]:
        """
        Resolve a challenge reference to a challenge_id.
        Reference can be:
        - An exact challenge_id (e.g., "c_a72893")
        - A challenge type (e.g., "pushups", "plank")

        Returns challenge_id or None if not found
        """
        reference = reference.strip().lower()

        # Get all active challenges for this user
        challenges = manager.list_challenges(discord_id, active_only=True)

        if not challenges:
            return None

        # First check if it's an exact challenge_id match
        for ch in challenges:
            if ch.challenge_id == reference:
                return ch.challenge_id

        # If not, treat it as a challenge type
        matching = [ch for ch in challenges if ch.challenge_type.lower() == reference]

        if len(matching) == 1:
            return matching[0].challenge_id
        elif len(matching) > 1:
            # Multiple challenges of same type - return None and let caller handle error
            return None

        return None

    @tree.command(name="log", description="Log progress for today (or a specific day)")
    @app_commands.describe(
        amount="How many reps/seconds/steps you did",
        challenge_id="Challenge ID or type (e.g., 'c_a72893' or 'pushups')",
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
        # Defer immediately to prevent interaction timeout
        await interaction.response.defer(ephemeral=True)

        try:
            p = manager.get_participant(str(interaction.user.id))
            if not p:
                await interaction.followup.send("❌ You're not in the challenge yet. Use **/join** first.", ephemeral=True)
                return

            tz_name = normalize_timezone(p.timezone, default=app_config.challenge.default_timezone)
            tz = pytz.timezone(tz_name)

            if log_date:
                d = _as_date(log_date)
            else:
                d = datetime.now(tz).date()

            # Resolve challenge ID or type
            if challenge_id:
                cid = _resolve_challenge_reference(p.discord_id, challenge_id)
                if not cid:
                    # Check if they have multiple challenges of the same type
                    challenges = manager.list_challenges(p.discord_id, active_only=True)
                    matching_type = [ch for ch in challenges if ch.challenge_type.lower() == challenge_id.lower()]

                    if len(matching_type) > 1:
                        # Show available challenge IDs for this type
                        ids = ", ".join([f"`{ch.challenge_id}` ({ch.daily_target} {ch.unit})" for ch in matching_type])
                        await interaction.followup.send(
                            f"❌ You have multiple **{challenge_id}** challenges. Please specify which one:\n{ids}",
                            ephemeral=True
                        )
                        return
                    else:
                        await interaction.followup.send(
                            f"❌ No active challenge found for `{challenge_id}`. Use `/challenge list` to see your challenges.",
                            ephemeral=True
                        )
                        return
            else:
                # Use default challenge if no reference provided
                cid = manager.resolve_default_challenge_id(p)
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

            # Note: Role syncing is handled by the scheduler when checking compliance
            # to avoid excessive Google Sheets API reads during high-traffic periods

            # Get challenge details for better feedback
            challenge_name = "legacy log"
            if cid:
                challenges = manager.list_challenges(p.discord_id, active_only=True)
                matching = [ch for ch in challenges if ch.challenge_id == cid]
                if matching:
                    ch = matching[0]
                    challenge_name = f"{ch.challenge_type} ({ch.daily_target} {ch.unit})"

            await interaction.followup.send(
                f"✅ Logged **{amount}** for **{d.isoformat()}**"
                + (f" - {challenge_name}" if challenge_name else ""),
                ephemeral=True,
            )
        except Exception as e:
            await interaction.followup.send(f"❌ {e}", ephemeral=True)

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
        await interaction.response.defer(ephemeral=True)
        try:
            p = manager.get_participant(str(interaction.user.id))
            if not p:
                await interaction.followup.send("❌ Use **/join** first.", ephemeral=True)
                return
            ch = manager.add_challenge(
                discord_id=p.discord_id,
                challenge_type=challenge_type,
                daily_target=daily_target,
                unit=unit,
                set_default=set_default,
            )
            await interaction.followup.send(
                f"✅ Added challenge: **{ch.challenge_type}** — target **{ch.daily_target} {ch.unit}**\n"
                f"ID: `{ch.challenge_id}`" + (" (default)" if set_default else ""),
                ephemeral=True,
            )
        except Exception as e:
            await interaction.followup.send(f"❌ {e}", ephemeral=True)

    @challenge_group.command(name="list", description="List your active challenges")
    async def challenge_list(interaction: discord.Interaction) -> None:
        await interaction.response.defer(ephemeral=True)
        try:
            p = manager.get_participant(str(interaction.user.id))
            if not p:
                await interaction.followup.send("❌ Use **/join** first.", ephemeral=True)
                return
            items = manager.list_challenges(p.discord_id, active_only=True)
            if not items:
                await interaction.followup.send("You have no active challenges yet. Add one with **/challenge add**.", ephemeral=True)
                return

            default_id = manager.resolve_default_challenge_id(p)
            lines = ["**Your Active Challenges:**\n"]
            for c in items:
                tag = " ⭐ default" if default_id and c.challenge_id == default_id else ""
                lines.append(f"• **{c.challenge_type}**: {c.daily_target} {c.unit}{tag}")
                lines.append(f"  💡 Log with: `/log amount:{c.daily_target} challenge_id:{c.challenge_type}`")
                lines.append("")

            lines.append("💡 **Tip:** You can use the challenge type (e.g., 'pushups', 'plank') instead of the ID when logging!")

            await interaction.followup.send("\n".join(lines), ephemeral=True)
        except Exception as e:
            await interaction.followup.send(f"❌ {e}", ephemeral=True)

    @challenge_group.command(name="remove", description="Deactivate a challenge")
    @app_commands.describe(challenge_id="Challenge ID or type (e.g., 'c_a72893' or 'pushups')")
    async def challenge_remove(interaction: discord.Interaction, challenge_id: str) -> None:
        await interaction.response.defer(ephemeral=True)
        try:
            p = manager.get_participant(str(interaction.user.id))
            if not p:
                await interaction.followup.send("❌ Use **/join** first.", ephemeral=True)
                return

            # Resolve challenge reference
            cid = _resolve_challenge_reference(p.discord_id, challenge_id)
            if not cid:
                await interaction.followup.send(
                    f"❌ No active challenge found for `{challenge_id}`. Use `/challenge list` to see your challenges.",
                    ephemeral=True
                )
                return

            ok = manager.remove_challenge(discord_id=p.discord_id, challenge_id=cid)
            await interaction.followup.send("✅ Removed." if ok else "❌ Could not remove.", ephemeral=True)
        except Exception as e:
            await interaction.followup.send(f"❌ {e}", ephemeral=True)

    @challenge_group.command(name="setdefault", description="Set your default challenge for /log")
    @app_commands.describe(challenge_id="Challenge ID or type (e.g., 'pushups')")
    async def challenge_setdefault(interaction: discord.Interaction, challenge_id: str) -> None:
        await interaction.response.defer(ephemeral=True)
        try:
            p = manager.get_participant(str(interaction.user.id))
            if not p:
                await interaction.followup.send("❌ Use **/join** first.", ephemeral=True)
                return

            # Resolve challenge reference
            cid = _resolve_challenge_reference(p.discord_id, challenge_id)
            if not cid:
                await interaction.followup.send(
                    f"❌ No active challenge found for `{challenge_id}`. Use `/challenge list` to see your challenges.",
                    ephemeral=True
                )
                return

            manager.set_default_challenge(discord_id=p.discord_id, challenge_id=cid)

            # Get challenge details for better feedback
            challenges = manager.list_challenges(p.discord_id, active_only=True)
            matching = [ch for ch in challenges if ch.challenge_id == cid]
            challenge_name = matching[0].challenge_type if matching else cid

            await interaction.followup.send(f"✅ Default challenge set to **{challenge_name}**.", ephemeral=True)
        except Exception as e:
            await interaction.followup.send(f"❌ {e}", ephemeral=True)

    # ---------------- /admin (group) ----------------
    admin_group = app_commands.Group(name="admin", description="Admin controls (requires Manage Server)")

    def _is_admin(interaction: discord.Interaction) -> bool:
        if not interaction.user or not isinstance(interaction.user, discord.Member):
            return False
        return interaction.user.guild_permissions.manage_guild

    @admin_group.command(name="set_mode", description="Set compliance mode: strict | lenient | points")
    @app_commands.describe(mode="strict, lenient, or points")
    async def admin_set_mode(interaction: discord.Interaction, mode: str) -> None:
        await interaction.response.defer(ephemeral=True)
        if not _is_admin(interaction):
            await interaction.followup.send("❌ You need **Manage Server** to run this.", ephemeral=True)
            return
        try:
            m = manager.set_compliance_mode(mode)
            await interaction.followup.send(f"✅ Compliance mode set to **{m}**.", ephemeral=True)
        except Exception as e:
            await interaction.followup.send(f"❌ {e}", ephemeral=True)

    @admin_group.command(name="set_points_target", description="In points mode, set how many challenges must be completed per day")
    @app_commands.describe(points="Minimum points per day (>=1)")
    async def admin_set_points(interaction: discord.Interaction, points: int) -> None:
        await interaction.response.defer(ephemeral=True)
        if not _is_admin(interaction):
            await interaction.followup.send("❌ You need **Manage Server** to run this.", ephemeral=True)
            return
        try:
            t = manager.set_points_target(points)
            await interaction.followup.send(f"✅ Points target set to **{t}**.", ephemeral=True)
        except Exception as e:
            await interaction.followup.send(f"❌ {e}", ephemeral=True)

    @admin_group.command(name="mode", description="Show current compliance mode settings")
    async def admin_mode(interaction: discord.Interaction) -> None:
        await interaction.response.defer(ephemeral=True)
        try:
            mode = manager.compliance_mode()
            pts = manager.points_target()

            mode_explanations = {
                "strict": "**strict** - Must complete ALL active challenges daily",
                "lenient": "**lenient** - Must complete ANY ONE active challenge daily",
                "points": "**points** - Earn 1 point per challenge; must reach points target"
            }

            explanation = mode_explanations.get(mode, "Unknown mode")

            await interaction.followup.send(
                f"**Current Mode:** `{mode}`\n"
                f"{explanation}\n\n"
                f"**Points Target:** `{pts}` (only used in points mode)",
                ephemeral=True,
            )
        except Exception as e:
            await interaction.followup.send(f"❌ {e}", ephemeral=True)

    @admin_group.command(name="setup_roles", description="Automatically create standard roles for the challenge bot")
    async def admin_setup_roles(interaction: discord.Interaction) -> None:
        if not _is_admin(interaction):
            await interaction.followup.send("❌ You need **Manage Server** to run this.", ephemeral=True)
            return

        if not interaction.guild:
            await interaction.followup.send("❌ This command must be used in a server.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True)

        try:
            guild = interaction.guild
            created_roles = []
            skipped_roles = []

            # Define roles to create: (name, color, reason)
            roles_to_create = [
                # Status roles
                ("Challenge Participant", discord.Color.blue(), "For all active challenge participants"),
                ("Compliant", discord.Color.green(), "Currently meeting daily targets"),
                ("Non-Compliant", discord.Color.red(), "Not meeting daily targets"),
                ("Male Group", discord.Color.dark_blue(), "Male participants"),
                ("Female Group", discord.Color.purple(), "Female participants"),

                # Streak achievement roles
                ("🔥 7 Day Streak", discord.Color.orange(), "Completed 7 consecutive days"),
                ("🔥 30 Day Streak", discord.Color.gold(), "Completed 30 consecutive days"),
                ("🔥 100 Day Streak", discord.Color.from_rgb(255, 215, 0), "Completed 100 consecutive days"),

                # Performance achievement roles
                ("⭐ Perfect Week", discord.Color.from_rgb(135, 206, 250), "7 consecutive compliant days"),
                ("⭐ Perfect Month", discord.Color.from_rgb(65, 105, 225), "30 consecutive compliant days"),
                ("💪 Overachiever", discord.Color.from_rgb(255, 140, 0), "Consistently exceeds targets"),

                # Milestone achievement roles
                ("🏆 1K Club", discord.Color.from_rgb(192, 192, 192), "1,000 total reps logged"),
                ("🏆 10K Club", discord.Color.from_rgb(255, 215, 0), "10,000 total reps logged"),
                ("🏆 100K Club", discord.Color.from_rgb(255, 215, 0), "100,000 total reps logged"),

                # Special achievement roles
                ("🌟 Early Bird", discord.Color.from_rgb(255, 255, 153), "Logs before 8 AM consistently"),
                ("🎯 Never Miss", discord.Color.from_rgb(50, 205, 50), "Zero punishments in 30 days"),
                ("👑 Challenge Champion", discord.Color.from_rgb(218, 165, 32), "Top performer of the month"),
            ]

            for role_name, role_color, reason in roles_to_create:
                # Check if role already exists
                existing_role = discord.utils.get(guild.roles, name=role_name)
                if existing_role:
                    skipped_roles.append(role_name)
                    continue

                # Create the role
                try:
                    new_role = await guild.create_role(
                        name=role_name,
                        color=role_color,
                        reason=f"Auto-setup by challenge bot: {reason}",
                        mentionable=True
                    )
                    created_roles.append(role_name)
                    LOGGER.info(f"Created role: {role_name}")
                except discord.Forbidden:
                    await interaction.followup.send("❌ Bot doesn't have permission to create roles. Grant 'Manage Roles' permission.", ephemeral=True)
                    return
                except Exception as e:
                    LOGGER.error(f"Failed to create role {role_name}: {e}")
                    await interaction.followup.send(f"❌ Failed to create role '{role_name}': {e}", ephemeral=True)
                    return

            # Build response message
            response_parts = []
            if created_roles:
                response_parts.append(f"✅ **Created {len(created_roles)} role(s):**\n" + "\n".join(f"• {r}" for r in created_roles))
            if skipped_roles:
                response_parts.append(f"ℹ️ **Skipped {len(skipped_roles)} existing role(s):**\n" + "\n".join(f"• {r}" for r in skipped_roles))

            if not created_roles and not skipped_roles:
                response_parts.append("No roles were created.")

            await interaction.followup.send("\n\n".join(response_parts), ephemeral=True)

        except Exception as e:
            LOGGER.error(f"Error in setup_roles: {e}")
            await interaction.followup.send(f"❌ An error occurred: {e}", ephemeral=True)

    @admin_group.command(name="leaderboard", description="Manually post the leaderboard to the leaderboard channel")
    async def admin_leaderboard(interaction: discord.Interaction) -> None:
        if not _is_admin(interaction):
            await interaction.followup.send("❌ You need **Manage Server** to run this.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True)

        try:
            from datetime import datetime
            import pytz

            scheduler = bot.scheduler
            if not scheduler:
                await interaction.followup.send("❌ Scheduler not available", ephemeral=True)
                return

            # Get today's date
            default_tz = pytz.timezone(app_config.challenge.default_timezone)
            today = datetime.now(default_tz).date()

            # Check if leaderboard channel is configured
            if not app_config.bot.leaderboards_channel_id:
                await interaction.followup.send(
                    "❌ Leaderboard channel not configured. Set LEADERBOARDS_CHANNEL_ID in Railway.",
                    ephemeral=True
                )
                return

            channel = bot.get_channel(app_config.bot.leaderboards_channel_id)
            if not channel:
                await interaction.followup.send(
                    f"❌ Could not find channel with ID {app_config.bot.leaderboards_channel_id}",
                    ephemeral=True
                )
                return

            # Fetch logs
            all_logs = scheduler._fetch_logs_for_leaderboard(target_date=today, is_global=False)

            # Get challenge types
            challenge_types = set()
            for discord_id, challenges in all_logs.items():
                challenge_types.update(challenges.keys())

            challenge_types = sorted(list(challenge_types))
            if not challenge_types:
                challenge_types = ['pushups']

            # Create paginated view
            view = scheduler.LeaderboardView(scheduler, today, challenge_types)

            # Build initial embed
            embed = scheduler._build_leaderboard_embed(
                date_obj=today,
                logs_data=all_logs,
                challenge_type=challenge_types[0],
                is_global=False,
                current_page=1,
                total_pages=len(challenge_types)
            )

            # Post to channel
            await channel.send(embed=embed, view=view)

            await interaction.followup.send(
                f"✅ Posted leaderboard to <#{app_config.bot.leaderboards_channel_id}>",
                ephemeral=True
            )

        except Exception as e:
            LOGGER.error(f"Error in admin_leaderboard: {e}")
            import traceback
            LOGGER.error(traceback.format_exc())
            await interaction.followup.send(f"❌ Failed to post leaderboard: {e}", ephemeral=True)

    # ---------------- /status ----------------
    @tree.command(name="status", description="Show your status for today (in your timezone)")
    async def status_cmd(interaction: discord.Interaction) -> None:
        # Defer immediately to prevent interaction timeout
        await interaction.response.defer(ephemeral=True)

        try:
            p = manager.get_participant(str(interaction.user.id))
            if not p:
                await interaction.followup.send("❌ Use **/join** first.", ephemeral=True)
                return

            tz_name = normalize_timezone(p.timezone, default=app_config.challenge.default_timezone)
            tz = pytz.timezone(tz_name)
            today = datetime.now(tz).date()

            st = manager.evaluate_multi_compliance(today).get(p.discord_id)
            if not st:
                await interaction.followup.send("❌ Couldn't compute status right now.", ephemeral=True)
                return

            # Build visual progress bar
            def make_progress_bar(current: int, target: int, length: int = 10) -> str:
                if target <= 0:
                    return "█" * length
                filled = int((current / target) * length)
                filled = min(filled, length)
                bar = "█" * filled + "░" * (length - filled)
                return bar

            if st.get("mode") == "legacy":
                met = (st.get("met") or [{}])[0]
                done = met.get('done', 0)
                target = met.get('target', 1)
                progress_bar = make_progress_bar(done, target)
                compliant = st.get('compliant')
                status_emoji = "✅" if compliant else "❌"

                msg = (
                    f"**Today:** `{today.isoformat()}`\n"
                    f"**Mode:** `legacy`\n"
                    f"**Progress:** `{done} / {target}` reps\n"
                    f"`{progress_bar}` {int((done/target)*100) if target > 0 else 0}%\n"
                    f"**Compliant:** {status_emoji} **{compliant}**\n"
                    f"**Missing:**\n" + ("None 🎉" if compliant else "Need more reps!")
                )
                await interaction.followup.send(msg, ephemeral=True)
                return

            mode = st.get("mode")
            points = st.get("points", 0)
            target = st.get("points_target", 1)
            progress_bar = make_progress_bar(points, target)
            compliant = st.get('compliant')
            status_emoji = "✅" if compliant else "❌"

            missing = st.get("missing") or []
            if missing:
                miss_lines = []
                for m in missing[:5]:
                    miss_lines.append(f"• {m.get('type')} — need {m.get('need')} {m.get('unit')} (`{m.get('challenge_id')}`)")
                miss_text = "\n".join(miss_lines)
            else:
                miss_text = "None 🎉"

            await interaction.followup.send(
                f"**Today:** `{today.isoformat()}`\n"
                f"**Mode:** `{mode}`\n"
                f"**Progress:** `{points} / {target}`\n"
                f"`{progress_bar}` {int((points/target)*100) if target > 0 else 0}%\n"
                f"**Compliant:** {status_emoji} **{compliant}**\n"
                f"**Missing:**\n{miss_text}",
                ephemeral=True,
            )
        except Exception as e:
            await interaction.followup.send(f"❌ {e}", ephemeral=True)

    # ---------------- /dayoff (simple) ----------------
    dayoff_group = app_commands.Group(name="dayoff", description="Request or vote for a day off")

    @dayoff_group.command(name="request", description="Request a day off (vote-based)")
    @app_commands.describe(target_day="YYYY-MM-DD (in your timezone)", reason="Optional reason")
    async def dayoff_request(interaction: discord.Interaction, target_day: str, reason: Optional[str] = None) -> None:
        await interaction.response.defer(ephemeral=True)
        try:
            p = manager.get_participant(str(interaction.user.id))
            if not p:
                await interaction.followup.send("❌ Use **/join** first.", ephemeral=True)
                return
            d = _as_date(target_day)
            deadline = datetime.utcnow().replace(tzinfo=pytz.UTC) + timedelta(hours=12)
            req = manager.create_day_off_request(
                requested_by=p.discord_id,
                target_day=d,
                reason=reason,
                deadline=deadline,
            )
            await interaction.followup.send(
                f"✅ Day-off request created for **{d.isoformat()}**.\n"
                f"Request ID: `{req.request_id}`\n"
                "Ask participants to vote with **/dayoff vote**.",
                ephemeral=True,
            )
        except Exception as e:
            await interaction.followup.send(f"❌ {e}", ephemeral=True)

    @dayoff_group.command(name="vote", description="Vote on a day-off request")
    @app_commands.describe(request_id="Request ID", vote="yes or no")
    async def dayoff_vote(interaction: discord.Interaction, request_id: str, vote: str) -> None:
        await interaction.response.defer(ephemeral=True)
        try:
            p = manager.get_participant(str(interaction.user.id))
            if not p:
                await interaction.followup.send("❌ Use **/join** first.", ephemeral=True)
                return
            manager.register_vote(request_id=request_id, voter_id=p.discord_id, vote=vote)
            await interaction.followup.send("✅ Vote recorded.", ephemeral=True)
        except Exception as e:
            await interaction.followup.send(f"❌ {e}", ephemeral=True)

    @dayoff_group.command(name="status", description="Check vote status for a request")
    @app_commands.describe(request_id="Request ID")
    async def dayoff_status(interaction: discord.Interaction, request_id: str) -> None:
        await interaction.response.defer(ephemeral=True)
        try:
            s = manager.compute_vote_state(request_id)
            await interaction.followup.send(
                f"Request `{request_id}` — state: **{s['state']}** (yes {s['yes']} / no {s['no']} / total {s['total']}, threshold {s['threshold']})",
                ephemeral=True,
            )
        except Exception as e:
            await interaction.followup.send(f"❌ {e}", ephemeral=True)

    tree.add_command(challenge_group)
    tree.add_command(admin_group)
    tree.add_command(dayoff_group)
