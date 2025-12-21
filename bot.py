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


def run() -> None:
    bot = ChallengeBot()
    bot.run(bot.app_config.bot.token)


if __name__ == "__main__":
    run()
