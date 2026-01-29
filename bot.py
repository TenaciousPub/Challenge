"""Entry point and top-level wiring for the daily challenge Discord bot."""

from __future__ import annotations

import asyncio
import logging

import discord

from .challenge_manager import ChallengeManager
from .commands import register_command_groups
from .config import load_config
from .sheets import GoogleSheetsService
from .workouts import WorkoutCatalog
from .scheduler import ComplianceScheduler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
)
LOGGER = logging.getLogger(__name__)


class ChallengeBot(discord.Client):
    def __init__(self) -> None:
        intents = discord.Intents.default()
        intents.members = True
        intents.reactions = True
        intents.message_content = True
        super().__init__(intents=intents)
        self.tree = discord.app_commands.CommandTree(self)

        self.app_config = load_config()
        self.sheets = GoogleSheetsService(self.app_config.sheets)
        self.workouts = WorkoutCatalog(self.sheets)
        self.manager = ChallengeManager(app_config=self.app_config, sheets=self.sheets, workouts=self.workouts)

        self.scheduler = ComplianceScheduler(self, self.manager, self.app_config)

    async def setup_hook(self) -> None:
        register_command_groups(self, self.manager, self.app_config)
        # Sync commands globally (or to one guild if you set GUILD_ID)
        try:
            if self.app_config.bot.guild_id:
                guild = discord.Object(id=int(self.app_config.bot.guild_id))
                self.tree.copy_global_to(guild=guild)
                await self.tree.sync(guild=guild)
                LOGGER.info("Synced commands to guild %s", self.app_config.bot.guild_id)
            else:
                await self.tree.sync()
                LOGGER.info("Synced commands globally")
        except Exception as e:
            LOGGER.warning("Command sync failed: %s", e)

    async def on_ready(self) -> None:
        LOGGER.info("Logged in as %s", self.user)
        self.scheduler.start()

    async def on_reaction_add(self, reaction: discord.Reaction, user: discord.User) -> None:
        """Handle reaction-based voting for day-off requests"""
        # Ignore bot's own reactions
        if user.bot:
            return

        await self._process_vote_reaction(reaction, user, is_add=True)

    async def on_reaction_remove(self, reaction: discord.Reaction, user: discord.User) -> None:
        """Handle reaction removal for day-off requests"""
        # Ignore bot's own reactions
        if user.bot:
            return

        await self._process_vote_reaction(reaction, user, is_add=False)

    async def _process_vote_reaction(
        self, reaction: discord.Reaction, user: discord.User, is_add: bool
    ) -> None:
        """Process a vote reaction (add or remove)"""
        # Only care about thumbs up/down
        if str(reaction.emoji) not in ["✅", "❌"]:
            return

        # Find the request associated with this message
        message_id = reaction.message.id
        request_id = None
        req = None

        for rid, r in self.manager._day_off_requests.items():
            if r.message_id == message_id:
                request_id = rid
                req = r
                break

        if not request_id or not req:
            # Not a vote message we care about
            return

        # Convert emoji to vote
        vote = "yes" if str(reaction.emoji) == "✅" else "no"

        # Register the vote
        voter_id = str(user.id)

        try:
            if is_add:
                # Check if user already has a different vote - allow them to change it
                dv = req.votes.get(voter_id)
                if dv and dv.vote in {"yes", "no"} and dv.vote != vote:
                    # User is changing their vote - reset first
                    dv.vote = "pending"
                    dv.voted_at = None
                    self.manager.sheets.update_day_off_vote(dv, target_day=req.target_day, reason=req.reason)
                    LOGGER.info(f"User {user.name} changing vote from {dv.vote} to {vote}")

                # Now register the new vote
                try:
                    self.manager.register_vote(
                        request_id=request_id,
                        voter_id=voter_id,
                        vote=vote
                    )
                    LOGGER.info(f"User {user.name} voted {vote} on request {request_id} via reaction")
                except RuntimeError as e:
                    # If they already voted this way, just ignore (they clicked the same reaction twice)
                    if "already voted" in str(e).lower():
                        LOGGER.debug(f"User {user.name} already voted {vote}, ignoring duplicate reaction")
                    else:
                        raise
            else:
                # Reset vote to pending when reaction is removed
                dv = req.votes.get(voter_id)
                if dv and dv.vote == vote:
                    dv.vote = "pending"
                    dv.voted_at = None
                    self.manager.sheets.update_day_off_vote(dv, target_day=req.target_day, reason=req.reason)
                    LOGGER.info(f"User {user.name} removed {vote} vote on request {request_id}")

            # Update the message with current vote counts
            await self._update_vote_message(reaction.message, request_id, req)

            # Check if vote reached threshold and auto-post results
            state = self.manager.compute_vote_state(request_id)
            if state["state"] == "approved" and not req.results_posted and self.app_config.bot.dayoff_results_channel_id:
                channel = self.get_channel(self.app_config.bot.dayoff_results_channel_id)
                if channel:
                    await self._post_vote_results(channel, request_id, req, state)
                    req.results_posted = True  # Mark as posted to prevent duplicates

        except RuntimeError as e:
            # Vote failed (ineligible, deadline passed, etc.)
            LOGGER.warning(f"Vote reaction failed for {user.name}: {e}")
            # Remove the invalid reaction
            try:
                await reaction.remove(user)
            except Exception:
                pass

    async def _update_vote_message(
        self, message: discord.Message, request_id: str, req
    ) -> None:
        """Update the vote message with current vote counts"""
        try:
            state = self.manager.compute_vote_state(request_id)

            # Build updated message
            reason_text = f"\n💬 **Reason:** {req.reason}" if req.reason else ""
            deadline_str = req.deadline.strftime("%Y-%m-%d %H:%M UTC")

            status_text = ""
            if state["state"] == "approved":
                status_text = "\n\n✅ **VOTE PASSED** - Day-off approved!"
            elif state["state"] == "rejected":
                status_text = "\n\n❌ **VOTE FAILED** - Not enough support."

            updated_content = (
                f"🗳️ **Day-Off Vote Started**\n\n"
                f"📅 **Date Requested:** {req.target_day.isoformat()}\n"
                f"🙋 **Requested by:** <@{req.requested_by}>{reason_text}\n"
                f"⏰ **Voting Deadline:** {deadline_str}\n"
                f"🆔 **Request ID:** `{request_id}`\n\n"
                f"**Current Votes:**\n"
                f"✅ Yes: {state['yes']} | ❌ No: {state['no']}\n\n"
                f"**React to vote:**\n"
                f"✅ = Yes | ❌ = No\n"
                f"_(Or use `/dayoff vote {request_id} yes/no`)_"
                f"{status_text}"
            )

            await message.edit(content=updated_content)
        except Exception as e:
            LOGGER.error(f"Failed to update vote message: {e}")

    async def _post_vote_results(
        self, channel: discord.TextChannel, request_id: str, req, state: dict
    ) -> None:
        """Post vote results to the dayoff results channel"""
        try:
            requester_mention = f"<@{req.requested_by}>"

            if state["state"] == "approved":
                result_emoji = "🎉"
                result_text = f"{result_emoji} **APPROVED** - No logging required on {req.target_day.isoformat()}!"
                ping_text = "<@&1458306967016701974> " if state["yes"] >= 3 else ""
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
            LOGGER.info(f"Posted results for request {request_id}: {state['state']}")
        except Exception as e:
            LOGGER.error(f"Failed to post vote results: {e}")


def run() -> None:
    bot = ChallengeBot()
    bot.run(bot.app_config.bot.token)


if __name__ == "__main__":
    run()
