import asyncio
import logging
from io import BytesIO
from typing import Sequence

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from aiogram import Bot, Dispatcher, Router
from aiogram.filters import Command
from aiogram.types import BufferedInputFile, Message
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from zoneinfo import ZoneInfo

from bot.config import BotConfig, load_config
from db_manager import (
    get_archive_average,
    get_current_trend,
    get_daily_top3_from_latest,
    get_latest_current_snapshot,
    init_db,
)
from utils.logger import setup_logging

LOGGER = logging.getLogger(__name__)
router = Router()


def _message_args(message: Message) -> list[str]:
    text = (message.text or "").strip()
    parts = text.split()
    return parts[1:] if len(parts) > 1 else []


def _parse_chart_args(args: list[str]) -> tuple[str | None, int]:
    """
    Parse /chart arguments:
    - /chart <checkpoint> [days]
    - checkpoint can contain spaces (days then should be last numeric token)
    """
    if not args:
        return None, 7

    days = 7
    if args and args[-1].isdigit():
        days = int(args[-1])
        args = args[:-1]
    checkpoint = " ".join(args).strip()
    return (checkpoint if checkpoint else None), days


def _build_line_chart(checkpoint: str, days: int) -> bytes:
    rows = get_current_trend(checkpoint=checkpoint, days=days)
    if not rows:
        raise ValueError("No data found for chart.")

    x_values = [row["timestamp"] for row in rows]
    cars = [row["cars_out"] for row in rows]
    trucks = [row["trucks_out"] for row in rows]
    buses = [row["buses_out"] for row in rows]

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(x_values, cars, label="Cars", linewidth=2)
    ax.plot(x_values, trucks, label="Trucks", linewidth=2)
    ax.plot(x_values, buses, label="Buses", linewidth=2)
    ax.set_title(f"Queue trend: {checkpoint} ({days} days)")
    ax.set_xlabel("Timestamp")
    ax.set_ylabel("Queue length")
    ax.legend()
    ax.grid(True, alpha=0.3)
    ax.tick_params(axis="x", rotation=45)
    fig.tight_layout()

    buffer = BytesIO()
    fig.savefig(buffer, format="png", dpi=150)
    plt.close(fig)
    buffer.seek(0)
    return buffer.getvalue()


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


@router.message(Command("start"))
async def cmd_start(message: Message) -> None:
    await message.answer(
        "Belarus Border Queue Tracker bot is running.\n\n"
        "Available commands:\n"
        "/queue - latest queue snapshot\n"
        "/history <checkpoint> - average queue by transport for last 7 days\n"
        "/chart <checkpoint> [days] - queue trend chart\n\n"
        "Examples:\n"
        "/history Брест\n"
        "/chart Каменный Лог 14"
    )


@router.message(Command("queue"))
async def cmd_queue(message: Message) -> None:
    rows = get_latest_current_snapshot()
    if not rows:
        await message.answer("No current queue data in database yet.")
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
    await message.answer(header + line + body + footer, parse_mode="HTML")


@router.message(Command("history"))
async def cmd_history(message: Message) -> None:
    args = _message_args(message)
    if not args:
        await message.answer("Usage: /history <checkpoint>\nExample: /history Брест")
        return

    checkpoint = " ".join(args).strip()
    rows = get_archive_average(checkpoint=checkpoint, days=7)
    if not rows:
        await message.answer(f"No archive data found for checkpoint: {checkpoint}")
        return

    lines = [f"Average queue for {checkpoint} (last 7 days):"]
    for row in rows:
        lines.append(f"- {row['transport_type']}: {row['avg_queue']}")
    await message.answer("\n".join(lines))


@router.message(Command("chart"))
async def cmd_chart(message: Message) -> None:
    args = _message_args(message)
    checkpoint, days = _parse_chart_args(args)
    if not checkpoint:
        await message.answer(
            "Usage: /chart <checkpoint> [days]\n"
            "Examples:\n"
            "/chart Брест\n"
            "/chart Каменный Лог 14"
        )
        return

    try:
        image_bytes = _build_line_chart(checkpoint, days)
    except ValueError as error:
        await message.answer(str(error))
        return

    file = BufferedInputFile(image_bytes, filename=f"{checkpoint}_{days}d.png")
    await message.answer_photo(file, caption=f"Queue trend for {checkpoint} ({days} days)")


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
    dp = Dispatcher()
    dp.include_router(router)

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
