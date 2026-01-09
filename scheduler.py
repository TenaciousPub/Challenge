from __future__ import annotations

import asyncio
import logging
import os
import random
from datetime import datetime, date, time as dtime, timedelta
from typing import Optional, Set, Tuple

import discord
import pytz

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

        # Gemini with rate limiting
        self.gemini_client = None
        self._gemini_last_call = 0.0  # Track last API call time
        self._gemini_min_interval = 2.0  # Minimum 2 seconds between calls

        # Compliance cache to prevent excessive Google Sheets API reads
        self._compliance_cache = {}  # {day_key: {compliance_data, timestamp}}
        self._compliance_cache_ttl = 300  # 5 minutes cache

        api_key = os.getenv("GEMINI_API_KEY", "").strip()
        if not api_key:
            LOGGER.warning("❌ GEMINI_API_KEY not set; Gemini DMs will use fallbacks")
        elif not genai:
            LOGGER.warning("❌ google-genai not installed; Gemini DMs will use fallbacks")
        else:
            try:
                self.gemini_client = genai.Client(api_key=api_key)
                LOGGER.info("✅ Gemini configured successfully for DMs")
            except Exception as e:
                LOGGER.warning("❌ Failed to configure Gemini: %s", e)

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

    async def _call_gemini_with_rate_limit(self, prompt: str) -> Optional[str]:
        """Call Gemini API with rate limiting to avoid 429 errors"""
        if not self.gemini_client:
            return None

        # Rate limiting: ensure minimum interval between calls
        import time
        now = time.time()
        time_since_last = now - self._gemini_last_call
        if time_since_last < self._gemini_min_interval:
            await asyncio.sleep(self._gemini_min_interval - time_since_last)

        try:
            self._gemini_last_call = time.time()
            resp = await asyncio.to_thread(
                self.gemini_client.models.generate_content,
                model='gemini-2.0-flash-exp',
                contents=prompt
            )
            return (resp.text or "").strip()
        except Exception as e:
            LOGGER.debug("Gemini API call failed: %s", e)
            return None

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

        text = await self._call_gemini_with_rate_limit(MOTIVATION_PROMPT)
        if not text:
            text = "Keep going—you've got this!"

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

        # Build personalized prompt for AI
        summary = status.get("summary", "")
        challenges_completed = status.get("completed_challenges", [])

        # Build context for AI
        context_parts = [
            CONGRATS_PROMPT,
            f"\nUser: {display_name}",
            f"Completion: {summary}" if summary else "",
        ]

        # Add challenge details if available
        if challenges_completed:
            challenge_details = ", ".join([f"{c.get('type', 'challenge')}" for c in challenges_completed[:3]])
            context_parts.append(f"Challenges completed: {challenge_details}")

        personalized_prompt = "\n".join([p for p in context_parts if p])
        text = await self._call_gemini_with_rate_limit(personalized_prompt)

        if not text:
            text = "Nice work—goal hit for today. Keep that streak alive!"

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
        """Post morning check-in message to #daily-checkins"""
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

            # Get today's targets for participants
            participants = self.manager.get_participants()
            if not participants:
                message = "☀️ **Good morning!** Time to crush your goals today!"
            else:
                message = (
                    "☀️ **Good morning, challengers!**\n\n"
                    "Time to log your progress! Use `/log` to record your work.\n\n"
                    "💪 Let's make today count!"
                )

            await channel.send(message)
            self._channel_post_flags.add(flag)
            LOGGER.info("Posted daily check-in to channel %s", channel_id)
        except Exception as e:
            LOGGER.warning("Failed to post daily check-in: %s", e)
            self._channel_post_flags.add(flag)

    async def _post_daily_leaderboard(self, day_key: str) -> None:
        """Post daily leaderboard with embed to #leaderboards"""
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

            # Get today's date and calculate compliance (using cache)
            today = datetime.now(pytz.timezone(self.app_config.challenge.default_timezone)).date()
            compliance_data = self._get_cached_compliance(today)

            # Build daily leaderboard - ONLY count valid participants
            daily_participants = []
            for discord_id, status in compliance_data.items():
                p = self.manager.get_participant_by_id(discord_id)
                if p:  # Only include if participant exists
                    daily_participants.append({
                        'discord_id': discord_id,
                        'name': p.display_name,
                        'compliant': bool(status.get('compliant')),
                        'progress': status.get('summary', ''),
                    })

            # Sort by compliance (compliant first), then alphabetically
            daily_participants.sort(key=lambda x: (not x['compliant'], x['name']))

            # Calculate stats
            compliant_count = sum(1 for p in daily_participants if p['compliant'])
            total_count = len(daily_participants)
            compliance_rate = int((compliant_count / total_count * 100)) if total_count > 0 else 0

            # Build global leaderboard (all-time stats)
            all_participants = self.manager.get_participants()
            global_participants = []
            for p in all_participants:
                try:
                    # Get total compliant days (you can expand this with more stats)
                    # For now, just show participants
                    global_participants.append({
                        'name': p.display_name,
                        'timezone': p.timezone,
                    })
                except Exception:
                    continue

            # Create Discord embed with flair
            embed = discord.Embed(
                title="🏆 Daily Challenge Leaderboard",
                description=f"**{today.strftime('%A, %B %d, %Y')}**",
                color=discord.Color.gold()
            )

            # Daily Stats Field
            stats_emoji = "🔥" if compliance_rate >= 80 else "💪" if compliance_rate >= 50 else "📊"
            embed.add_field(
                name=f"{stats_emoji} Today's Performance",
                value=(
                    f"**{compliant_count}/{total_count}** compliant "
                    f"({compliance_rate}%)\n"
                    f"{'━' * 20}"
                ),
                inline=False
            )

            # Compliant participants
            if compliant_count > 0:
                compliant_names = [
                    f"✅ **{p['name']}**"
                    for p in daily_participants
                    if p['compliant']
                ]
                compliant_text = "\n".join(compliant_names[:15])  # Limit to 15 to fit
                if len(compliant_names) > 15:
                    compliant_text += f"\n... and {len(compliant_names) - 15} more"

                embed.add_field(
                    name="✅ Crushing It Today",
                    value=compliant_text,
                    inline=True
                )

            # Non-compliant participants
            non_compliant = [p for p in daily_participants if not p['compliant']]
            if non_compliant:
                non_compliant_names = [f"❌ {p['name']}" for p in non_compliant[:15]]
                non_compliant_text = "\n".join(non_compliant_names)
                if len(non_compliant) > 15:
                    non_compliant_text += f"\n... and {len(non_compliant) - 15} more"

                embed.add_field(
                    name="⏰ Still Time Left",
                    value=non_compliant_text,
                    inline=True
                )

            # Global Stats Field
            embed.add_field(
                name="🌍 Challenge Stats",
                value=(
                    f"**Total Participants:** {len(global_participants)}\n"
                    f"**Active Today:** {total_count}\n"
                    f"Use `/status` to check your progress!"
                ),
                inline=False
            )

            # Footer with motivational message
            footer_messages = [
                "Every rep counts! Keep pushing! 💪",
                "Consistency beats perfection! 🔥",
                "The only bad workout is the one you didn't do! ⚡",
                "You're building something great! 🎯",
                "Small daily improvements = Big results! 🚀",
            ]
            embed.set_footer(text=random.choice(footer_messages))

            await channel.send(embed=embed)
            self._channel_post_flags.add(flag)
            LOGGER.info("Posted daily leaderboard embed to channel %s", channel_id)
        except Exception as e:
            LOGGER.warning("Failed to post daily leaderboard: %s", e)
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
            text = await self._call_gemini_with_rate_limit(TEAM_MOTIVATION_PROMPT)
            if not text:
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
                text = random.choice(messages)

            await channel.send(f"💪 **Daily Motivation**\n\n{text}")
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
