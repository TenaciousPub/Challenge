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

    # ---------------- /join (group) ----------------
    join_group = app_commands.Group(name="join", description="Join or edit your challenge profile")

    @join_group.command(name="start", description="Join the daily challenge")
    @app_commands.describe(
        gender="male or female (required)",
        is_disabled="true if you need chair/floor-friendly punishments (required)",
        timezone="IANA tz like America/Los_Angeles or PST/EST (required)"
    )
    async def join_start_cmd(
        interaction: discord.Interaction,
        gender: str,
        is_disabled: bool,
        timezone: str,
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

    @join_group.command(name="edit", description="Edit your profile (gender, is_disabled, timezone)")
    @app_commands.describe(
        gender="Update gender (male or female)",
        is_disabled="Update disability status (true for chair/floor-friendly punishments)",
        timezone="Update timezone (IANA tz like America/Los_Angeles or PST/EST)"
    )
    async def join_edit_cmd(
        interaction: discord.Interaction,
        gender: Optional[str] = None,
        is_disabled: Optional[bool] = None,
        timezone: Optional[str] = None,
    ) -> None:
        await interaction.response.defer(ephemeral=True)
        try:
            discord_id = str(interaction.user.id)
            p = manager.get_participant(discord_id)
            if not p:
                await interaction.followup.send(
                    "❌ You haven't joined yet. Use **/join start** first.",
                    ephemeral=True
                )
                return

            # Check if at least one field is provided
            if gender is None and is_disabled is None and timezone is None:
                await interaction.followup.send(
                    "❌ Please provide at least one field to update (gender, is_disabled, or timezone).",
                    ephemeral=True
                )
                return

            updated_fields = []

            # Update gender
            if gender is not None:
                manager.sheets.update_participant_field(discord_id, "gender", gender)
                updated_fields.append(f"gender: **{gender}**")

                # Update gender role if in a guild
                if interaction.guild and isinstance(interaction.user, discord.Member):
                    try:
                        # Remove old gender roles
                        for role in interaction.user.roles:
                            if role.id in [role_ids.ROLE_MALE_GROUP, role_ids.ROLE_FEMALE_GROUP]:
                                await interaction.user.remove_roles(role, reason="Gender updated")

                        # Add new gender role
                        gender_role_id = role_ids.get_gender_role_id(gender)
                        if gender_role_id:
                            gender_role = interaction.guild.get_role(gender_role_id)
                            if gender_role and gender_role not in interaction.user.roles:
                                await interaction.user.add_roles(gender_role, reason=f"Gender updated to: {gender}")
                    except Exception as e:
                        LOGGER.warning(f"Failed to update gender roles for {interaction.user}: {e}")

            # Update is_disabled
            if is_disabled is not None:
                manager.sheets.update_participant_field(discord_id, "is_disabled", str(is_disabled))
                updated_fields.append(f"is_disabled: **{is_disabled}**")

            # Update timezone
            if timezone is not None:
                tz = normalize_timezone(timezone, default=app_config.challenge.default_timezone)
                manager.sheets.update_participant_field(discord_id, "timezone", tz)
                updated_fields.append(f"timezone: **{tz}**")

            await interaction.followup.send(
                f"✅ **Profile Updated**\n" + "\n".join(f"• {field}" for field in updated_fields),
                ephemeral=True
            )
        except Exception as e:
            LOGGER.error(f"Error in join edit: {e}")
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
                await interaction.followup.send("❌ You're not in the challenge yet. Use **/join start** first.", ephemeral=True)
                return

            tz_name = normalize_timezone(p.timezone, default=app_config.challenge.default_timezone)
            tz = pytz.timezone(tz_name)

            if log_date:
                d = _as_date(log_date)
            else:
                d = datetime.now(tz).date()

            # Check if this date is an approved day-off
            if manager.has_approved_dayoff(participant_id=p.discord_id, local_day=d):
                await interaction.followup.send(
                    f"🎉 **{d.isoformat()} is an approved day-off!**\n\n"
                    "No logging needed. Enjoy your rest! 😊",
                    ephemeral=True
                )
                return

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
                await interaction.followup.send("❌ Use **/join start** first.", ephemeral=True)
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
                await interaction.followup.send("❌ Use **/join start** first.", ephemeral=True)
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
                await interaction.followup.send("❌ Use **/join start** first.", ephemeral=True)
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
                await interaction.followup.send("❌ Use **/join start** first.", ephemeral=True)
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

            # Get ALL challenge types from Challenges sheet (not just ones with data)
            challenge_types = set()
            try:
                all_challenges = manager.sheets.fetch_challenges(active_only=True)
                for ch in all_challenges:
                    challenge_types.add(ch.challenge_type)
            except Exception as e:
                LOGGER.warning(f"Failed to fetch challenge types: {e}")

            # Also add any challenge types from logged data
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
                await interaction.followup.send("❌ Use **/join start** first.", ephemeral=True)
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
                await interaction.followup.send("❌ Use **/join start** first.", ephemeral=True)
                return
            d = _as_date(target_day)
            deadline = datetime.utcnow().replace(tzinfo=pytz.UTC) + timedelta(hours=12)
            req = manager.create_day_off_request(
                requested_by=p.discord_id,
                target_day=d,
                reason=reason,
                deadline=deadline,
            )

            # Post to public channel if configured
            if app_config.bot.dayoff_results_channel_id:
                try:
                    channel = bot.get_channel(app_config.bot.dayoff_results_channel_id)
                    if channel:
                        reason_text = f"\n**Reason:** {reason}" if reason else ""
                        deadline_str = deadline.astimezone(pytz.timezone(app_config.challenge.default_timezone)).strftime("%I:%M %p %Z")

                        await channel.send(
                            f"🗳️ **Day-Off Vote Started**\n\n"
                            f"📅 **Date Requested:** {d.isoformat()}\n"
                            f"🙋 **Requested by:** <@{p.discord_id}>{reason_text}\n"
                            f"⏰ **Voting Deadline:** {deadline_str}\n"
                            f"🆔 **Request ID:** `{req.request_id}`\n\n"
                            f"**To vote, use:** `/dayoff vote {req.request_id} yes` or `no`"
                        )
                except Exception as e:
                    LOGGER.error(f"Failed to post day-off request to channel: {e}")

            await interaction.followup.send(
                f"✅ Day-off request created for **{d.isoformat()}**.\n"
                f"Request ID: `{req.request_id}`\n"
                f"Posted to voting channel for all participants to vote.",
                ephemeral=True,
            )
        except Exception as e:
            await interaction.followup.send(f"❌ {e}", ephemeral=True)

    async def _post_vote_results(request_id: str, req, state: dict) -> None:
        """Post vote results to the dayoff results channel"""
        if not app_config.bot.dayoff_results_channel_id:
            return

        try:
            channel = bot.get_channel(app_config.bot.dayoff_results_channel_id)
            if not channel:
                return

            # Find the requester's display name
            requester_mention = f"<@{req.requested_by}>"

            # Build result message
            if state["state"] == "approved":
                result_emoji = "🎉"
                result_text = f"{result_emoji} **APPROVED** - No logging required on {req.target_day.isoformat()}!"
                ping_text = "@everyone " if state["yes"] >= 3 else ""
            else:
                result_emoji = "❌"
                result_text = f"{result_emoji} **REJECTED** - Regular challenge requirements apply on {req.target_day.isoformat()}."
                ping_text = ""

            message = (
                f"{ping_text}🗳️ **Day-Off Vote Results**\n\n"
                f"📅 **Date Requested:** {req.target_day.isoformat()}\n"
                f"🙋 **Requested by:** {requester_mention}\n\n"
                f"✅ **Yes:** {state['yes']} votes\n"
                f"❌ **No:** {state['no']} votes\n\n"
                f"{result_text}"
            )

            if state["state"] == "approved":
                message += "\n\n━━━━━━━━━━━━━━━━━━━━━━━━━\nAll participants get a free day. Enjoy your rest!"

            await channel.send(message)
            LOGGER.info(f"Posted vote results for {request_id}: {state['state']}")
        except Exception as e:
            LOGGER.error(f"Failed to post vote results: {e}")

    @dayoff_group.command(name="vote", description="Vote on a day-off request")
    @app_commands.describe(request_id="Request ID", vote="yes or no")
    async def dayoff_vote(interaction: discord.Interaction, request_id: str, vote: str) -> None:
        await interaction.response.defer(ephemeral=True)
        try:
            p = manager.get_participant(str(interaction.user.id))
            if not p:
                await interaction.followup.send("❌ Use **/join start** first.", ephemeral=True)
                return

            # Register the vote
            manager.register_vote(request_id=request_id, voter_id=p.discord_id, vote=vote)

            # Check if vote reached threshold
            state = manager.compute_vote_state(request_id)
            req = manager._day_off_requests.get(request_id)

            # Send confirmation
            await interaction.followup.send(
                f"✅ Vote recorded: **{vote}**\n\n"
                f"**Current Status:**\n"
                f"✅ Yes: {state['yes']} | ❌ No: {state['no']}\n"
                f"**State:** {state['state']}",
                ephemeral=True
            )

            # Post results if approved or rejected
            if state["state"] in ["approved", "rejected"] and req:
                await _post_vote_results(request_id, req, state)

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

    # ---------------- /nutrition ----------------
    # Helper functions for unit conversions
    def lbs_to_kg(lbs: float) -> float:
        """Convert pounds to kilograms"""
        return lbs * 0.453592

    def ft_in_to_cm(feet: int, inches: int) -> float:
        """Convert feet and inches to centimeters"""
        total_inches = (feet * 12) + inches
        return total_inches * 2.54

    @tree.command(name="nutrition", description="Talk to your AI nutrition coach (restricted channel)")
    @app_commands.describe(
        message="Your message to the nutrition coach",
        height_ft="Your height in feet (for profile setup)",
        height_in="Your height in inches (for profile setup)",
        weight_lbs="Your weight in pounds (for profile setup)",
        goal="Your fitness goal (for profile setup)"
    )
    async def nutrition_cmd(
        interaction: discord.Interaction,
        message: str,
        height_ft: Optional[int] = None,
        height_in: Optional[int] = None,
        weight_lbs: Optional[float] = None,
        goal: Optional[str] = None
    ) -> None:
        await interaction.response.defer()

        try:
            # Check channel restriction
            nutrition_channel_id = 1458307023111323739
            if interaction.channel_id != nutrition_channel_id:
                await interaction.followup.send(
                    f"❌ Nutrition coach is only available in <#{nutrition_channel_id}>",
                    ephemeral=True
                )
                return

            # Check if scheduler has AI
            scheduler = bot.scheduler
            if not scheduler or (not scheduler.claude_client and not scheduler.gemini_client):
                await interaction.followup.send(
                    "❌ AI nutrition coach is not available. Please contact an admin.",
                    ephemeral=True
                )
                return

            discord_id = str(interaction.user.id)
            display_name = interaction.user.display_name

            # Get or create nutrition profile from sheets
            user_profile = None
            try:
                ws = manager.sheets._worksheet("NutritionProfiles")
                from .sheets import _safe_get_all_records
                profiles = _safe_get_all_records(ws, expected_headers=["discord_id", "display_name", "height_cm", "weight_kg", "goal", "last_updated"])

                for p in profiles:
                    if str(p.get('discord_id', '')) == discord_id:
                        user_profile = p
                        LOGGER.info(f"Found nutrition profile for {display_name}: {user_profile}")
                        break
            except Exception as e:
                LOGGER.warning(f"Failed to fetch nutrition profile: {e}")
                user_profile = None

            # Update profile if new data provided
            if height_ft is not None or height_in is not None or weight_lbs is not None or goal is not None:
                # Convert units
                height_cm = None
                weight_kg = None

                if height_ft is not None and height_in is not None:
                    height_cm = ft_in_to_cm(height_ft, height_in)

                if weight_lbs is not None:
                    weight_kg = lbs_to_kg(weight_lbs)

                # Update sheet
                try:
                    ws = manager.sheets._worksheet("NutritionProfiles")
                    from .sheets import _safe_get_all_records
                    profiles = _safe_get_all_records(ws, expected_headers=["discord_id", "display_name", "height_cm", "weight_kg", "goal", "last_updated"])

                    # Find or create row
                    row_idx = None
                    for idx, p in enumerate(profiles):
                        if str(p.get('discord_id', '')) == discord_id:
                            row_idx = idx + 2  # +2 for header and 1-indexing
                            break

                    from datetime import datetime
                    timestamp = datetime.now().isoformat()

                    if row_idx:
                        # Update existing
                        if height_cm:
                            ws.update_cell(row_idx, 3, round(height_cm, 1))
                        if weight_kg:
                            ws.update_cell(row_idx, 4, round(weight_kg, 1))
                        if goal:
                            ws.update_cell(row_idx, 5, goal)
                        ws.update_cell(row_idx, 6, timestamp)
                    else:
                        # Add new row
                        ws.append_row([
                            discord_id,
                            display_name,
                            round(height_cm, 1) if height_cm else "",
                            round(weight_kg, 1) if weight_kg else "",
                            goal if goal else "",
                            timestamp
                        ])

                    await interaction.followup.send(
                        f"✅ **Profile Updated**\n"
                        f"Height: {height_ft}'{height_in}\" ({round(height_cm, 1)} cm)\n"
                        f"Weight: {weight_lbs} lbs ({round(weight_kg, 1)} kg)\n"
                        f"Goal: {goal if goal else 'Not set'}\n\n"
                        f"Now ask your nutrition question!",
                        ephemeral=True
                    )
                    return
                except Exception as e:
                    LOGGER.error(f"Failed to update nutrition profile: {e}")
                    await interaction.followup.send(f"❌ Failed to update profile: {e}", ephemeral=True)
                    return

            # Build context from profile
            context = ""
            if user_profile:
                height = user_profile.get('height_cm', '')
                weight = user_profile.get('weight_kg', '')
                user_goal = user_profile.get('goal', '')

                if height or weight or user_goal:
                    context = f"\nUser Profile:\n- Height: {height} cm\n- Weight: {weight} kg\n- Goal: {user_goal}\n"

            # Build nutrition coach prompt
            nutrition_prompt = f"""You are an expert fitness nutrition coach specializing in evidence-based nutrition advice for athletes and fitness enthusiasts. You provide personalized, practical advice.

{context}

User's Question: {message}

Provide a detailed, personalized response in 2-3 paragraphs. Consider their profile if available. Focus on practical, actionable advice. If discussing specific medical conditions, recommend consulting a healthcare professional. Be encouraging and supportive."""

            # Call AI with fallback chain
            response, provider = await scheduler._call_ai_with_rate_limit(nutrition_prompt)

            if not response:
                await interaction.followup.send(
                    "❌ Failed to generate nutrition advice. Please try again later.",
                    ephemeral=True
                )
                return

            # Send response with provider label
            await interaction.followup.send(
                f"🥗 **Nutrition Coach** _{provider}_\n\n{response}\n\n*Note: This is AI-generated advice. Consult a healthcare professional for personalized medical guidance.*"
            )

        except Exception as e:
            LOGGER.error(f"Error in nutrition command: {e}")
            import traceback
            LOGGER.error(traceback.format_exc())
            await interaction.followup.send(f"❌ An error occurred: {e}", ephemeral=True)

    tree.add_command(join_group)
    tree.add_command(challenge_group)
    tree.add_command(admin_group)
    tree.add_command(dayoff_group)
