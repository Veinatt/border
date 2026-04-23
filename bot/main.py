import asyncio
import logging
from io import BytesIO
from typing import Sequence

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import BufferedInputFile, Message
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from zoneinfo import ZoneInfo

from bot.chart_fsm import answer_slash_chart, chart_router, get_main_keyboard
from bot.config import BotConfig, load_config
from db_manager import (
    get_archive_average,
    get_daily_top3_from_latest,
    get_latest_current_snapshot,
    init_db,
)
from utils.logger import setup_logging

LOGGER = logging.getLogger(__name__)
router = Router()

# Long /start + help copy (English, matches existing bot tone).
_START_HELP_TEXT = (
    "Belarus Border Queue Tracker\n\n"
    "**Reply keyboard**\n"
    "• 🚗 Current Queue — latest snapshot from the scraper\n"
    "• 📊 Chart — step-by-step chart (checkpoint, period, optional time filter)\n"
    "• 📈 7-Day History — averages from unified daily stats\n"
    "• ❓ Help — this text\n\n"
    "**Commands (still supported)**\n"
    "/queue — same as 🚗 Current Queue\n"
    "/history <checkpoint> — 7-day averages (e.g. /history Брест)\n"
    "/chart — open chart wizard (same as 📊 Chart)\n"
    "/chart <checkpoint> [days] — last N days (default 7)\n"
    "/chart <checkpoint> YYYY-MM-DD YYYY-MM-DD — custom date range\n"
    "/chart <checkpoint> YYYY-MM-DD YYYY-MM-DD HH:MM HH:MM — range + time-of-day filter\n\n"
    "Examples:\n"
    "/history Брест\n"
    "/chart Каменный Лог 14\n"
    "/chart Брест 2026-04-01 2026-04-20\n"
    "/chart Брест 2026-04-01 2026-04-20 08:00 20:00"
)


def _message_args(message: Message) -> list[str]:
    text = (message.text or "").strip()
    parts = text.split()
    return parts[1:] if len(parts) > 1 else []


async def _send_queue_snapshot(message: Message) -> None:
    """Format and send the latest current_queue snapshot (shared by /queue and menu)."""
    rows = get_latest_current_snapshot()
    if not rows:
        await message.answer(
            "No current queue data in database yet.",
            reply_markup=get_main_keyboard(),
        )
        return

    header = "<pre>Checkpoint            Cars  Trucks  Buses\n"
    line = "-" * 42 + "\n"
    body = ""
    for row in rows:
        body += (
            f"{row['checkpoint'][:20]:20} "
            f"{row['cars_out']:>5} {row['trucks_out']:>7} {row['buses_out']:>6}\n"
        )
    footer = "</pre>"
    await message.answer(
        header + line + body + footer,
        parse_mode="HTML",
        reply_markup=get_main_keyboard(),
    )


def _build_top3_chart(top_rows: Sequence) -> bytes:
    labels = [row["checkpoint"] for row in top_rows]
    values = [row["cars_out"] for row in top_rows]

    fig, ax = plt.subplots(figsize=(7, 4))
    ax.bar(labels, values)
    ax.set_title("Top 3 checkpoints by cars queue")
    ax.set_ylabel("Cars queue")
    ax.grid(axis="y", alpha=0.3)
    fig.tight_layout()

    buffer = BytesIO()
    fig.savefig(buffer, format="png", dpi=150)
    plt.close(fig)
    buffer.seek(0)
    return buffer.getvalue()


@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext) -> None:
    """Reset FSM and show main reply keyboard."""
    await state.clear()
    await message.answer(
        _START_HELP_TEXT,
        parse_mode="Markdown",
        reply_markup=get_main_keyboard(),
    )


@router.message(Command("queue"))
async def cmd_queue(message: Message) -> None:
    await _send_queue_snapshot(message)


@router.message(F.text == "🚗 Current Queue")
async def menu_current_queue(message: Message) -> None:
    await _send_queue_snapshot(message)


@router.message(F.text == "❓ Help")
async def menu_help(message: Message, state: FSMContext) -> None:
    """Repeat help; clearing FSM avoids stuck chart wizard."""
    await state.clear()
    await message.answer(
        _START_HELP_TEXT,
        parse_mode="Markdown",
        reply_markup=get_main_keyboard(),
    )


@router.message(Command("history"))
async def cmd_history(message: Message) -> None:
    args = _message_args(message)
    if not args:
        await message.answer(
            "Usage: /history <checkpoint>\nExample: /history Брест",
            reply_markup=get_main_keyboard(),
        )
        return

    checkpoint = " ".join(args).strip()
    rows = get_archive_average(checkpoint=checkpoint, days=7)
    if not rows:
        await message.answer(
            f"No archive data found for checkpoint: {checkpoint}",
            reply_markup=get_main_keyboard(),
        )
        return

    lines = [f"Average queue for {checkpoint} (last 7 days):"]
    for row in rows:
        lines.append(f"- {row['transport_type']}: {row['avg_queue']}")
    await message.answer("\n".join(lines), reply_markup=get_main_keyboard())


@router.message(Command("chart"))
async def cmd_chart(message: Message, state: FSMContext) -> None:
    """
    Backward-compatible /chart: text args parsed in chart_fsm; bare /chart starts FSM wizard.
    """
    parts = _message_args(message)
    try:
        await answer_slash_chart(message, state, parts)
    except Exception:
        LOGGER.exception("/chart handler failed")
        await message.answer(
            "Could not build chart. Check the format and try again.",
            reply_markup=get_main_keyboard(),
        )
        await state.clear()


async def _send_daily_summary(bot: Bot, config: BotConfig) -> None:
    if config.target_chat_id is None:
        LOGGER.info("Daily summary skipped: TARGET_CHAT_ID is not configured.")
        return

    top3 = get_daily_top3_from_latest()
    if not top3:
        await bot.send_message(
            chat_id=config.target_chat_id,
            message_thread_id=config.target_message_thread_id,
            text="Daily summary: no current queue data yet.",
        )
        return

    lines = ["Daily border queue summary (Top 3 by cars):"]
    for idx, row in enumerate(top3, start=1):
        lines.append(
            f"{idx}. {row['checkpoint']} - cars: {row['cars_out']}, "
            f"trucks: {row['trucks_out']}, buses: {row['buses_out']}"
        )

    await bot.send_message(
        chat_id=config.target_chat_id,
        message_thread_id=config.target_message_thread_id,
        text="\n".join(lines),
    )

    chart_bytes = _build_top3_chart(top3)
    chart_file = BufferedInputFile(chart_bytes, filename="daily_top3.png")
    await bot.send_photo(
        chat_id=config.target_chat_id,
        message_thread_id=config.target_message_thread_id,
        photo=chart_file,
        caption="Daily Top-3 checkpoints chart",
    )


async def run_bot() -> None:
    setup_logging("bot.log")
    init_db()
    config = load_config()

    bot = Bot(token=config.token)
    # MemoryStorage keeps FSM state in RAM (fine for a single bot process).
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)
    # Higher priority router: menu + chart FSM (see aiogram router order).
    dp.include_router(chart_router)

    scheduler = AsyncIOScheduler(timezone=ZoneInfo(config.timezone))
    scheduler.add_job(
        _send_daily_summary,
        trigger=CronTrigger(hour=9, minute=0),
        args=[bot, config],
        id="daily_summary_9am",
        replace_existing=True,
    )
    scheduler.start()
    LOGGER.info("Daily summary scheduler started at 09:00 (%s).", config.timezone)

    LOGGER.info("Bot polling started.")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(run_bot())
