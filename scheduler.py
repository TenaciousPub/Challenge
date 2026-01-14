from __future__ import annotations

import asyncio
import logging
import os
import random
from datetime import datetime, date, time as dtime, timedelta
from typing import Optional, Set, Tuple, List

import discord
import pytz

try:
    import anthropic  # type: ignore
except Exception:  # pragma: no cover
    anthropic = None

try:
    from google import genai  # type: ignore
except Exception:  # pragma: no cover
    genai = None

from .timezones import normalize_timezone
from .role_sync import sync_compliance_roles

LOGGER = logging.getLogger(__name__)

MOTIVATION_PROMPT = (
    "You are a supportive workout coach. Write a short (1–2 sentences), "
    "positive and encouraging message to motivate someone doing a daily challenge. "
    "Each time, make it slightly different."
)

TEAM_MOTIVATION_PROMPT = (
    "You are a supportive fitness coach addressing an entire team. Write a short (2-3 sentences) "
    "motivational message to inspire a group of people working together on a daily fitness challenge. "
    "Focus on team spirit, collective effort, and group accountability. Make it uplifting and energizing."
)

CONGRATS_PROMPT = (
    "You are a supportive coach congratulating someone who just completed their daily fitness goal. "
    "Write a short (2-3 sentences) personalized congratulations message. "
    "Use the details provided to make it specific and encouraging. "
    "Keep it upbeat, genuine, and not cheesy. Vary your message each time."
)


def _parse_hhmm(value: str, fallback: dtime) -> dtime:
    try:
        hh, mm = (value or "").strip().split(":")
        return dtime(int(hh), int(mm))
    except Exception:
        return fallback


class ComplianceScheduler:
    """
    Periodic jobs (timezone-aware):
      • Motivation DM at motivation_time_local (default 18:00) in participant tz.
      • Reminder DM at reminder_time_local (default 22:00) if they haven’t logged today.
      • Punishment check at punishment_run_time_local (default 00:05) in participant tz:
            checks YESTERDAY compliance; if missed and not already punished, DM punishment and mark.
      • Congrats DM when they become compliant for today (sent once per local day).
      • All DMs are skipped if an approved day-off exists for that participant’s local day.
    """

    def __init__(self, bot, manager, app_config) -> None:
        self.bot = bot
        self.manager = manager
        self.app_config = app_config
        self.task: Optional[asyncio.Task] = None

        # Avoid duplicate DMs: (participant_id, "YYYY-MM-DD", tag)
        self._sent_flags: Set[Tuple[str, str, str]] = set()
        self._punish_flags: Set[Tuple[str, str]] = set()      # (discord_id, yday_local)
        self._congrats_flags: Set[Tuple[str, str]] = set()    # (discord_id, day_key)

        # Channel posting flags: (channel_tag, "YYYY-MM-DD")
        self._channel_post_flags: Set[Tuple[str, str]] = set()

        self._motivation_time = _parse_hhmm(self.app_config.challenge.motivation_time_local, dtime(18, 0))
        self._reminder_time = _parse_hhmm(self.app_config.challenge.reminder_time_local, dtime(22, 0))
        self._punish_time = _parse_hhmm(self.app_config.challenge.punishment_run_time_local, dtime(0, 5))

        # Channel posting times (server timezone)
        self._daily_checkin_time = _parse_hhmm(self.app_config.challenge.daily_checkin_time, dtime(6, 0))
        self._leaderboard_time = _parse_hhmm(self.app_config.challenge.leaderboard_time, dtime(20, 0))

        # AI with fallback: Claude → Gemini → Local
        self.claude_client = None
        self.gemini_client = None
        self._ai_last_call = 0.0  # Track last API call time
        self._ai_min_interval = 1.0  # Minimum 1 second between calls

        # Compliance cache to prevent excessive Google Sheets API reads
        self._compliance_cache = {}  # {day_key: {compliance_data, timestamp}}
        self._compliance_cache_ttl = 300  # 5 minutes cache

        # Try to configure Claude (primary)
        claude_key = os.getenv("ANTHROPIC_API_KEY", "").strip()
        if claude_key and anthropic:
            try:
                self.claude_client = anthropic.Anthropic(api_key=claude_key)
                LOGGER.info("✅ Claude AI configured (primary)")
            except Exception as e:
                LOGGER.warning("❌ Failed to configure Claude: %s", e)

        # Try to configure Gemini (backup)
        gemini_key = os.getenv("GEMINI_API_KEY", "").strip()
        if gemini_key and genai:
            try:
                self.gemini_client = genai.Client(api_key=gemini_key)
                LOGGER.info("✅ Gemini AI configured (backup)")
            except Exception as e:
                LOGGER.warning("❌ Failed to configure Gemini: %s", e)

        if not self.claude_client and not self.gemini_client:
            LOGGER.warning("⚠️ No AI configured - will use local fallback messages")

    def _get_cached_compliance(self, day: date):
        """Get cached compliance data or fetch and cache if expired"""
        import time
        day_key = day.isoformat()
        now = time.time()

        # Check cache
        if day_key in self._compliance_cache:
            cache_entry = self._compliance_cache[day_key]
            if now - cache_entry['timestamp'] < self._compliance_cache_ttl:
                LOGGER.debug(f"Using cached compliance data for {day_key}")
                return cache_entry['data']

        # Cache miss or expired - fetch new data
        try:
            LOGGER.info(f"Fetching compliance data for {day_key} (cache miss)")
            data = self.manager.evaluate_multi_compliance(day)
            self._compliance_cache[day_key] = {
                'data': data,
                'timestamp': now
            }
            # Clean old cache entries (keep only last 3 days)
            old_keys = [k for k in self._compliance_cache.keys() if k < (day - timedelta(days=3)).isoformat()]
            for k in old_keys:
                del self._compliance_cache[k]
            return data
        except Exception as e:
            LOGGER.error(f"Failed to fetch compliance data: {e}")
            return {}

    async def _call_ai_with_rate_limit(self, prompt: str, fallback_messages: Optional[List[str]] = None) -> Tuple[Optional[str], str]:
        """
        Call AI with fallback chain: Claude → Gemini → Local
        Returns: (text, provider) where provider is "Claude", "Gemini", or "Fallback"
        """
        # Rate limiting: ensure minimum interval between calls
        import time
        now = time.time()
        time_since_last = now - self._ai_last_call
        if time_since_last < self._ai_min_interval:
            await asyncio.sleep(self._ai_min_interval - time_since_last)

        LOGGER.info(f"AI call attempt - Claude: {bool(self.claude_client)}, Gemini: {bool(self.gemini_client)}")

        # Try Claude first (primary)
        if self.claude_client:
            try:
                self._ai_last_call = time.time()
                response = await asyncio.to_thread(
                    lambda: self.claude_client.messages.create(
                        model="claude-3-5-haiku-20241022",
                        max_tokens=200,
                        messages=[{"role": "user", "content": prompt}]
                    )
                )
                if response.content and len(response.content) > 0:
                    text = response.content[0].text.strip()
                    if text:
                        LOGGER.info("✅ Claude API call successful")
                        return (text, "Claude")
            except Exception as e:
                LOGGER.warning(f"❌ Claude API call failed: {e}")
                import traceback
                LOGGER.warning(traceback.format_exc())

        # Try Gemini backup
        if self.gemini_client:
            try:
                self._ai_last_call = time.time()
                response = await asyncio.to_thread(
                    self.gemini_client.models.generate_content,
                    model='gemini-2.0-flash-exp',
                    contents=prompt
                )
                text = (response.text or "").strip()
                if text:
                    LOGGER.info("✅ Gemini API call successful")
                    return (text, "Gemini")
            except Exception as e:
                LOGGER.warning(f"❌ Gemini API call failed: {e}")
                import traceback
                LOGGER.warning(traceback.format_exc())

        # Local fallback
        if fallback_messages:
            return (random.choice(fallback_messages), "Fallback")

        return (None, "None")

    def start(self) -> None:
        if self.task is None:
            self.task = asyncio.create_task(self.loop())

    async def loop(self) -> None:
        await self.bot.wait_until_ready()
        LOGGER.info("Scheduler started")
        while not self.bot.is_closed():
            try:
                await self._tick_once()
            except Exception as e:
                LOGGER.exception("Scheduler tick error: %s", e)
            await asyncio.sleep(60)

    async def _tick_once(self) -> None:
        default_tz = pytz.timezone(self.app_config.challenge.default_timezone)
        now_server = datetime.now(default_tz).replace(second=0, microsecond=0)
        today_server = now_server.date()
        day_key = today_server.isoformat()

        # Channel posting (server timezone)
        # 1) Daily check-in message
        if now_server.time() == self._daily_checkin_time:
            await self._post_daily_checkin(day_key)

        # 2) Daily leaderboard
        if now_server.time() == self._leaderboard_time:
            await self._post_daily_leaderboard(day_key)

        # 3) Motivation channel message (post at same time as DMs)
        if now_server.time() == self._motivation_time:
            await self._post_motivation_message(day_key)

        participants = self.manager.get_participants()
        for idx, p in enumerate(participants):
            # Yield control every 3 participants to prevent event loop blocking
            if idx > 0 and idx % 3 == 0:
                await asyncio.sleep(0)

            tz_name = normalize_timezone(p.timezone, default=self.app_config.challenge.default_timezone)
            tz = pytz.timezone(tz_name)
            now_local = datetime.now(tz).replace(second=0, microsecond=0)
            today_local = now_local.date()
            day_key = today_local.isoformat()

            # Day-off skip (for today local)
            if self.manager.has_approved_dayoff(participant_id=p.discord_id, local_day=today_local):
                self._sent_flags.discard((p.discord_id, day_key, "motivation"))
                self._sent_flags.discard((p.discord_id, day_key, "reminder"))
                self._congrats_flags.discard((p.discord_id, day_key))
                continue

            # 1) Punishment at local midnight-ish (checks yesterday)
            if now_local.time() == self._punish_time:
                await self._maybe_run_local_midnight_punishment(
                    discord_id=p.discord_id,
                    display_name=p.display_name,
                    tz=tz,
                )

            # 2) Motivation at 18:00 local
            if now_local.time() == self._motivation_time:
                await self._maybe_send_motivation(
                    discord_id=p.discord_id,
                    display_name=p.display_name,
                    day_key=day_key,
                    window="motivation",
                    always=True,
                )

            # 3) Reminder at 22:00 local if no log yet today
            if now_local.time() == self._reminder_time:
                await self._maybe_send_motivation(
                    discord_id=p.discord_id,
                    display_name=p.display_name,
                    day_key=day_key,
                    window="reminder",
                    always=False,
                )

            # 4) Congrats DM (check only every 5 minutes to avoid blocking event loop)
            # Check on :00, :05, :10, :15, :20, :25, :30, :35, :40, :45, :50, :55
            if now_local.minute % 5 == 0:
                await self._maybe_send_congrats_if_completed(
                    discord_id=p.discord_id,
                    display_name=p.display_name,
                    local_day=today_local,
                )

    async def _maybe_send_motivation(
        self,
        *,
        discord_id: str,
        display_name: str,
        day_key: str,
        window: str,    # "motivation" | "reminder"
        always: bool,
    ) -> None:
        flag = (discord_id, day_key, window)
        if flag in self._sent_flags:
            return

        if window == "reminder" and not always:
            try:
                local_date = datetime.strptime(day_key, "%Y-%m-%d").date()
                totals = self.manager.sheets.daily_pushup_totals(local_date, include_bonus=True)
                if int(totals.get(discord_id, 0)) > 0:
                    self._sent_flags.add(flag)
                    return
            except Exception as e:
                LOGGER.debug("Reminder log check failed for %s: %s", display_name, e)

        # Use simple fallback messages to conserve API quota
        # (AI is reserved for team messages which are more impactful)
        fallback_messages = [
            "Keep going—you've got this!",
            "Push through today—you're stronger than you think! 💪",
            "One day at a time. Let's make this one count! 🔥",
            "Your future self will thank you for not giving up today! ⚡",
            "Progress over perfection. Get it done! 💯"
        ]
        text = random.choice(fallback_messages)

        try:
            user = self.bot.get_user(int(discord_id))
            if not user:
                self._sent_flags.add(flag)
                return
            prefix = "💪 Check-in" if window == "motivation" else "⏰ Reminder"
            await user.send(f"{prefix}: {text}")
            self._sent_flags.add(flag)
        except Exception as e:
            LOGGER.warning("Failed to DM %s to %s: %s", window, display_name, e)
            self._sent_flags.add(flag)

    async def _maybe_send_congrats_if_completed(
        self,
        *,
        discord_id: str,
        display_name: str,
        local_day: date,
    ) -> None:
        day_key = local_day.isoformat()
        flag = (discord_id, day_key)
        if flag in self._congrats_flags:
            return

        # Also avoid duplicates across restarts via sheet field
        try:
            last = self.manager.sheets.get_participant_field(discord_id, "last_congrats_on") or ""
            if str(last).strip() == day_key:
                self._congrats_flags.add(flag)
                return
        except Exception:
            pass

        # Check compliance using cache to prevent excessive API calls
        try:
            compliance_data = await asyncio.to_thread(self._get_cached_compliance, local_day)
            status = compliance_data.get(str(discord_id))
            if not status or not bool(status.get("compliant")):
                return
        except Exception as e:
            LOGGER.debug(f"Compliance check failed for {display_name}: {e}")
            return

        # Sync compliance roles
        if self.app_config.bot.guild_id:
            try:
                guild = self.bot.get_guild(self.app_config.bot.guild_id)
                if guild:
                    member = guild.get_member(int(discord_id))
                    if member:
                        await sync_compliance_roles(member, is_compliant=True)
            except Exception as e:
                LOGGER.warning(f"Failed to sync compliance roles for {discord_id}: {e}")

        # Use variety of congrats messages (conserve AI quota for team messages)
        congrats_messages = [
            "Nice work—goal hit for today. Keep that streak alive!",
            "You crushed it today! That's how it's done! 🔥",
            "Goals completed! You showed up and delivered! 💪",
            "Another win in the books! Keep building that momentum! ⚡",
            "Nailed it! Your consistency is paying off! 🎯",
            "Challenge completed! You're proving what you're made of! 💯"
        ]
        text = random.choice(congrats_messages)

        try:
            user = self.bot.get_user(int(discord_id))
            if user:
                await user.send(f"🎉 {text}")
        except Exception as e:
            LOGGER.warning("Failed to DM congrats to %s: %s", display_name, e)

        try:
            self.manager.sheets.update_participant_field(discord_id, "last_congrats_on", day_key)
        except Exception:
            pass
        self._congrats_flags.add(flag)

    async def _maybe_run_local_midnight_punishment(self, discord_id: str, display_name: str, tz: pytz.BaseTzInfo) -> None:
        """At local midnight window, check YESTERDAY compliance in user's TZ and assign punishment if needed."""
        now_local = datetime.now(tz)
        yday = (now_local.date() - timedelta(days=1))
        yday_key = yday.isoformat()

        # Optional: ignore days before CHALLENGE_START_DATE
        start_str = getattr(self.app_config.challenge, "start_date", None)
        if start_str:
            try:
                start_day = date.fromisoformat(start_str)
                if yday < start_day:
                    return
            except Exception:
                pass

        flag = (discord_id, yday_key)
        if flag in self._punish_flags:
            return

        # Check persisted last_punished_on
        try:
            last = self.manager.sheets.get_participant_field(discord_id, "last_punished_on") or ""
            if str(last).strip() == yday_key:
                self._punish_flags.add(flag)
                return
        except Exception:
            pass

        # Skip if approved day-off for that yday (local)
        try:
            if self.manager.has_approved_dayoff(participant_id=discord_id, local_day=yday):
                self._punish_flags.add(flag)
                return
        except Exception:
            pass

        # Check multi compliance for yesterday
        try:
            status = self.manager.evaluate_multi_compliance(yday).get(str(discord_id))
        except Exception:
            status = None

        if status and bool(status.get("compliant")):
            self._punish_flags.add(flag)
            return

        # Build human-readable summary
        missing = (status or {}).get("missing") or []
        summary_lines = []
        for m in missing[:5]:
            summary_lines.append(f"• {m.get('type')} — need {m.get('need')} {m.get('unit')}")
        summary = "\n".join(summary_lines) if summary_lines else "• You missed your goal."

        # Choose punishment: disabled -> floor/chair only; else any
        p = self.manager.get_participant_by_id(discord_id)
        punishment = None
        try:
            if p and p.is_disabled and hasattr(self.manager.workouts, "random_floor_or_chair"):
                punishment = self.manager.workouts.random_floor_or_chair()
            elif hasattr(self.manager.workouts, "random"):
                punishment = self.manager.workouts.random()
        except Exception:
            punishment = None

        accessible_fallback = [
            "🪑 Chair tricep dips — 3×10",
            "🪑 Seated leg raises — 3×15",
            "🪑 Wall pushups — 3×15",
            "🪑 Seated torso twists — 3×20",
            "🪑 Gentle chair yoga flow — 5 minutes",
            "🪑 Floor glute bridges — 3×15",
            "🪑 Seated punches — 3×30s",
            "🪑 Floor stretches + 2×15 wall pushups",
        ]

        punishment_text = None
        if punishment and getattr(punishment, "description", None):
            punishment_text = str(punishment.description).strip()
        if not punishment_text:
            punishment_text = random.choice(accessible_fallback) if (p and p.is_disabled) else "100 burpees — unbroken if possible 😈"

        # DM punishment
        try:
            user = self.bot.get_user(int(discord_id))
            if user:
                await user.send(
                    "😈 You missed your goal yesterday.\n\n"
                    f"{summary}\n\n"
                    f"Here's your punishment workout:\n**{punishment_text}**"
                )
        except Exception as e:
            LOGGER.warning("Failed to DM punishment to %s: %s", display_name, e)

        # Post punishment to channel
        await self._post_punishment_announcement(discord_id, display_name, punishment_text)

        # Mark punished (sheet + daily log)
        try:
            self.manager.sheets.update_participant_field(discord_id, "last_punished_on", yday_key)
        except Exception:
            pass
        try:
            self.manager.sheets.mark_penalized_for_day(discord_id, yday)
        except Exception:
            pass

        self._punish_flags.add(flag)

    async def _post_daily_checkin(self, day_key: str) -> None:
        """Post morning check-in message to #daily-checkins with AI-generated twist"""
        flag = ("daily_checkin", day_key)
        if flag in self._channel_post_flags:
            return

        channel_id = self.app_config.bot.daily_checkins_channel_id
        if not channel_id:
            return

        try:
            channel = self.bot.get_channel(channel_id)
            if not channel:
                return

            # Generate AI-powered daily message
            today = datetime.now(pytz.timezone(self.app_config.challenge.default_timezone))
            day_of_week = today.strftime("%A")

            daily_checkin_prompt = f"""Generate a brief, energizing morning check-in message for a fitness challenge group on {day_of_week}.
Keep it under 50 words, motivational but not cheesy. Include:
- A unique greeting (not just "good morning")
- Reference that it's {day_of_week}
- Encourage them to use /log to track progress
- End with a powerful call to action

Make it feel fresh, authentic, and pumped up. No generic quotes."""

            # Fallback messages
            fallback_messages = [
                f"☀️ **Rise and grind, challengers!**\n\nIt's {day_of_week}—time to turn goals into action. Use `/log` to record your progress.\n\n💪 Let's make today legendary!",
                f"⚡ **{day_of_week} energy incoming!**\n\nYour body is capable of amazing things. Show it what you've got today! Don't forget to `/log` your work.\n\n🔥 Push harder than yesterday!",
                f"🚀 **{day_of_week}, let's fly!**\n\nEvery rep counts. Every set matters. Track your journey with `/log`.\n\n💯 Commitment over comfort!",
                f"💥 **Happy {day_of_week}, warriors!**\n\nThe only bad workout is the one you didn't do. Get moving and `/log` that progress!\n\n🎯 Consistency builds champions!",
            ]

            ai_message, provider = await self._call_ai_with_rate_limit(daily_checkin_prompt, fallback_messages)

            if ai_message:
                message = f"☀️ **Daily Check-In** _{provider}_\n\n{ai_message}"
            else:
                message = random.choice(fallback_messages)

            await channel.send(message)
            self._channel_post_flags.add(flag)
            LOGGER.info("Posted daily check-in to channel %s", channel_id)
        except Exception as e:
            LOGGER.warning("Failed to post daily check-in: %s", e)
            self._channel_post_flags.add(flag)

    class LeaderboardView(discord.ui.View):
        """Paginated leaderboard with Daily/Global toggle"""

        def __init__(self, scheduler, today: date, challenge_types: List[str]):
            super().__init__(timeout=None)
            self.scheduler = scheduler
            self.today = today
            self.challenge_types = challenge_types if challenge_types else ['pushups']
            self.current_page = 0  # Index into challenge_types
            self.is_global = False  # False = Daily, True = Global

            self._update_buttons()

        def _update_buttons(self):
            """Rebuild buttons based on current state"""
            self.clear_items()

            # Row 1: Daily/Global toggle
            daily_btn = discord.ui.Button(
                label="📊 Daily",
                style=discord.ButtonStyle.primary if not self.is_global else discord.ButtonStyle.secondary,
                custom_id="leaderboard_daily",
                disabled=not self.is_global,
                row=0
            )
            daily_btn.callback = self._daily_callback

            global_btn = discord.ui.Button(
                label="🌍 All-Time",
                style=discord.ButtonStyle.primary if self.is_global else discord.ButtonStyle.secondary,
                custom_id="leaderboard_global",
                disabled=self.is_global,
                row=0
            )
            global_btn.callback = self._global_callback

            self.add_item(daily_btn)
            self.add_item(global_btn)

            # Row 2: Previous/Next page buttons (if multiple challenges)
            if len(self.challenge_types) > 1:
                prev_btn = discord.ui.Button(
                    label="◀️ Previous",
                    style=discord.ButtonStyle.secondary,
                    custom_id="leaderboard_prev",
                    disabled=self.current_page == 0,
                    row=1
                )
                prev_btn.callback = self._prev_callback

                next_btn = discord.ui.Button(
                    label="Next ▶️",
                    style=discord.ButtonStyle.secondary,
                    custom_id="leaderboard_next",
                    disabled=self.current_page >= len(self.challenge_types) - 1,
                    row=1
                )
                next_btn.callback = self._next_callback

                self.add_item(prev_btn)
                self.add_item(next_btn)

        async def _daily_callback(self, interaction: discord.Interaction):
            await interaction.response.defer()
            self.is_global = False
            await self._update_message(interaction)

        async def _global_callback(self, interaction: discord.Interaction):
            await interaction.response.defer()
            self.is_global = True
            await self._update_message(interaction)

        async def _prev_callback(self, interaction: discord.Interaction):
            await interaction.response.defer()
            if self.current_page > 0:
                self.current_page -= 1
            await self._update_message(interaction)

        async def _next_callback(self, interaction: discord.Interaction):
            await interaction.response.defer()
            if self.current_page < len(self.challenge_types) - 1:
                self.current_page += 1
            await self._update_message(interaction)

        async def _update_message(self, interaction: discord.Interaction):
            """Rebuild and update the leaderboard"""
            try:
                # Fetch logs
                all_logs = self.scheduler._fetch_logs_for_leaderboard(
                    target_date=self.today if not self.is_global else None,
                    is_global=self.is_global
                )

                # Get current challenge type
                current_challenge = self.challenge_types[self.current_page]

                # Build embed
                embed = self.scheduler._build_leaderboard_embed(
                    date_obj=self.today,
                    logs_data=all_logs,
                    challenge_type=current_challenge,
                    is_global=self.is_global,
                    current_page=self.current_page + 1,
                    total_pages=len(self.challenge_types)
                )

                # Update buttons
                self._update_buttons()

                # Update message
                await interaction.edit_original_response(embed=embed, view=self)

            except Exception as e:
                LOGGER.error(f"Failed to update leaderboard: {e}")
                import traceback
                LOGGER.error(traceback.format_exc())
                try:
                    await interaction.followup.send("❌ Failed to update leaderboard", ephemeral=True)
                except:
                    pass

    def _build_leaderboard_embed(
        self,
        date_obj: date,
        logs_data: dict,
        challenge_type: str,
        is_global: bool,
        current_page: int = 1,
        total_pages: int = 1
    ) -> discord.Embed:
        """Build a leaderboard embed for a specific challenge type"""

        # Build rankings for this challenge type
        rankings = []
        for discord_id, challenges in logs_data.items():
            if challenge_type in challenges:
                p = self.manager.get_participant_by_id(discord_id)
                if p:
                    rankings.append({
                        'name': p.display_name,
                        'amount': challenges[challenge_type]['amount'],
                        'unit': challenges[challenge_type]['unit']
                    })

        if not rankings:
            # No data for this challenge type
            title = f"{'🌍 All-Time' if is_global else '🏆 Daily'} Leaderboard - {challenge_type.title()}"
            description = f"**{date_obj.strftime('%A, %B %d, %Y')}**" if not is_global else "**All-Time Stats**"

            if total_pages > 1:
                description += f"\n\n📄 Page {current_page}/{total_pages}"

            embed = discord.Embed(
                title=title,
                description=description + "\n\n💪 No Data Yet\nBe the first to log today!",
                color=0xFFD700 if not is_global else 0x3498db
            )
            embed.set_footer(text="Use /log to add your progress!")
            return embed

        # Sort by amount (highest first)
        rankings.sort(key=lambda x: x['amount'], reverse=True)

        # Create embed
        title = f"{'🌍 All-Time' if is_global else '🏆 Daily'} Leaderboard - {challenge_type.title()}"
        description = f"**{date_obj.strftime('%A, %B %d, %Y')}**" if not is_global else "**All-Time Stats**"

        if total_pages > 1:
            description += f"\n\n📄 Page {current_page}/{total_pages}"

        embed = discord.Embed(
            title=title,
            description=description,
            color=0xFFD700 if not is_global else 0x3498db
        )

        # Medal emojis for top 3
        medals = ["🥇", "🥈", "🥉"]
        leaderboard_lines = []

        # Show top 10
        for idx, entry in enumerate(rankings[:10]):
            medal = medals[idx] if idx < 3 else f"`#{idx+1}`"
            leaderboard_lines.append(
                f"{medal} **{entry['name']}** — {entry['amount']:,} {entry['unit']}"
            )

        embed.add_field(
            name="🏆 Top Performers",
            value="\n".join(leaderboard_lines),
            inline=False
        )

        # Stats
        total_amount = sum(r['amount'] for r in rankings)
        avg_amount = total_amount // len(rankings) if rankings else 0

        embed.add_field(
            name="📊 Stats",
            value=(
                f"👥 **{len(rankings)}** participant{'s' if len(rankings) != 1 else ''}\n"
                f"📈 **Total:** {total_amount:,} {rankings[0]['unit']}\n"
                f"💯 **Average:** {avg_amount:,} {rankings[0]['unit']}"
            ),
            inline=False
        )

        embed.set_footer(text="Use /log to add your progress!")

        return embed

    def _fetch_logs_for_leaderboard(self, target_date: Optional[date] = None, is_global: bool = False) -> dict:
        """Fetch and aggregate logs for leaderboard display"""
        all_logs = {}
        try:
            # Build challenge lookup dict
            challenge_lookup = {}
            try:
                all_challenges = self.manager.sheets.fetch_challenges(active_only=False)
                for ch in all_challenges:
                    challenge_lookup[ch.challenge_id] = ch
            except Exception as e:
                LOGGER.warning(f"Failed to fetch challenges for lookup: {e}")

            # Access the DailyLog worksheet directly
            ws = self.manager.sheets._worksheet("DailyLog")

            # Fetch all records - use old schema with pushup_count
            from .sheets import _safe_get_all_records
            expected_headers = ["date", "discord_id", "pushup_count", "workout_bonus", "penalized", "notes", "logged_at", "challenge_id"]
            daily_logs = _safe_get_all_records(ws, expected_headers=expected_headers)

            for log in daily_logs:
                log_date = log.get('date', '')

                # Filter by date if not global view
                if not is_global and target_date:
                    if log_date != target_date.isoformat():
                        continue

                discord_id = str(log.get('discord_id', ''))
                if not discord_id:
                    continue

                # Get challenge info from challenge_id
                challenge_id = log.get('challenge_id', '')
                if challenge_id and challenge_id in challenge_lookup:
                    challenge = challenge_lookup[challenge_id]
                    challenge_type = challenge.challenge_type
                    unit = challenge.unit
                else:
                    # Fallback for old data without challenge_id
                    if challenge_id:
                        LOGGER.warning(f"⚠️ Challenge ID '{challenge_id}' not found in lookup for log on {log_date}. Defaulting to pushups. Check for typos in DailyLog sheet!")
                    challenge_type = 'pushups'
                    unit = 'reps'

                # Handle amount - old schema uses pushup_count
                try:
                    pushup_count = int(log.get('pushup_count', 0))
                    workout_bonus = int(log.get('workout_bonus', 0)) if log.get('workout_bonus') else 0
                    amount = pushup_count + workout_bonus
                except (ValueError, TypeError):
                    amount = 0

                if discord_id not in all_logs:
                    all_logs[discord_id] = {}
                if challenge_type not in all_logs[discord_id]:
                    all_logs[discord_id][challenge_type] = {'amount': 0, 'unit': unit}
                all_logs[discord_id][challenge_type]['amount'] += amount

        except Exception as e:
            LOGGER.warning(f"Failed to fetch daily logs: {e}")
            import traceback
            LOGGER.warning(traceback.format_exc())

        return all_logs

    async def _post_daily_leaderboard(self, day_key: str) -> None:
        """Post paginated leaderboard with Daily/Global toggle"""
        flag = ("leaderboard", day_key)
        if flag in self._channel_post_flags:
            return

        channel_id = self.app_config.bot.leaderboards_channel_id
        if not channel_id:
            return

        try:
            channel = self.bot.get_channel(channel_id)
            if not channel:
                return

            # Get today's date
            today = datetime.now(pytz.timezone(self.app_config.challenge.default_timezone)).date()

            # Get all logs for today
            all_logs = self._fetch_logs_for_leaderboard(target_date=today, is_global=False)

            # Get ALL challenge types from Challenges sheet (not just ones with data)
            challenge_types = set()
            try:
                all_challenges = self.manager.sheets.fetch_challenges(active_only=True)
                for ch in all_challenges:
                    challenge_types.add(ch.challenge_type)
                LOGGER.info(f"Found {len(challenge_types)} challenge types: {challenge_types}")
            except Exception as e:
                LOGGER.warning(f"Failed to fetch challenge types: {e}")

            # Also add any challenge types from logged data
            for discord_id, challenges in all_logs.items():
                challenge_types.update(challenges.keys())

            challenge_types = sorted(list(challenge_types))

            if not challenge_types:
                challenge_types = ['pushups']  # Default fallback

            LOGGER.info(f"Leaderboard will show {len(challenge_types)} pages: {challenge_types}")

            # Create interactive view with pagination
            view = self.LeaderboardView(self, today, challenge_types)

            # Build initial embed (first challenge, daily view)
            embed = self._build_leaderboard_embed(
                date_obj=today,
                logs_data=all_logs,
                challenge_type=challenge_types[0],
                is_global=False,
                current_page=1,
                total_pages=len(challenge_types)
            )

            await channel.send(embed=embed, view=view)
            self._channel_post_flags.add(flag)
            LOGGER.info("Posted paginated leaderboard to channel %s", channel_id)
        except Exception as e:
            LOGGER.warning(f"Failed to post daily leaderboard: {e}")
            LOGGER.exception("Leaderboard error details:")
            self._channel_post_flags.add(flag)

    async def _post_motivation_message(self, day_key: str) -> None:
        """Post motivational message to #motivation channel"""
        flag = ("motivation", day_key)
        if flag in self._channel_post_flags:
            return

        channel_id = self.app_config.bot.motivation_channel_id
        if not channel_id:
            return

        try:
            channel = self.bot.get_channel(channel_id)
            if not channel:
                return

            # Try to get AI-generated team motivation message
            messages = [
                "💪 Together we're stronger! Every person who shows up today makes our team better. Let's push each other to greatness!",
                "🔥 This team doesn't quit! We're all in this together—let's make today count as a unit!",
                "⚡ When one of us wins, we all win. Support each other, hold each other accountable, and let's crush these goals together!",
                "🎯 Great teams are built one rep at a time. Show up for yourself, show up for the team. We've got this!",
                "💯 The strength of the team is each individual member. The strength of each member is the team. Let's make today legendary!",
                "🏆 Accountability + Support = Unstoppable. That's who we are. Let's prove it today, challengers!",
                "🚀 We're not just individuals working out—we're a squad pushing limits together. Time to show what we're made of!",
                "💥 Your effort inspires others. Others' dedication fuels you. That's the power of this team. Let's go!",
            ]

            text, provider = await self._call_ai_with_rate_limit(TEAM_MOTIVATION_PROMPT, messages)

            if text:
                await channel.send(f"💪 **Daily Motivation** _{provider}_\n\n{text}")
            else:
                await channel.send(f"💪 **Daily Motivation**\n\n{random.choice(messages)}")
            self._channel_post_flags.add(flag)
            LOGGER.info("Posted motivation message to channel %s", channel_id)
        except Exception as e:
            LOGGER.warning("Failed to post motivation message: %s", e)
            self._channel_post_flags.add(flag)

    async def _post_punishment_announcement(self, discord_id: str, display_name: str, punishment_text: str) -> None:
        """Post punishment announcement to #punishment-wo... channel"""
        channel_id = self.app_config.bot.punishment_channel_id
        if not channel_id:
            return

        try:
            channel = self.bot.get_channel(channel_id)
            if not channel:
                return

            # Try to get the member to mention them
            guild = self.bot.get_guild(self.app_config.bot.guild_id) if self.app_config.bot.guild_id else None
            mention = f"<@{discord_id}>"

            message = (
                f"😈 **Punishment Assigned**\n\n"
                f"{mention} missed their goal yesterday.\n\n"
                f"**Punishment Workout:**\n{punishment_text}\n\n"
                f"💪 Time to make up for it!"
            )

            await channel.send(message)
            LOGGER.info("Posted punishment announcement for %s to channel %s", display_name, channel_id)
        except Exception as e:
            LOGGER.warning("Failed to post punishment announcement: %s", e)
