"""
FSM-driven chart wizard, shared inline keyboards, and chart image generation.

Uses checkpoints from scrapers.current_scraper.DEFAULT_CHECKPOINTS.
"""
from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta
from io import BytesIO
from typing import Any

import matplotlib

matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
from aiogram import F, Router
from aiogram.filters import StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    BufferedInputFile,
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
)

from db_manager import get_archive_average, get_current_queue_range
from scrapers.current_scraper import DEFAULT_CHECKPOINTS

LOGGER = logging.getLogger(__name__)

chart_router = Router(name="chart_fsm")


def _callback_host_message(query: CallbackQuery) -> Message | None:
    """Return the host message for inline callbacks, or None if not editable."""
    host = query.message
    return host if isinstance(host, Message) else None

# --- Callback data prefixes (keep short for Telegram 64-byte limit) ---
CB_CHART_CP = "chart_cp:"
CB_PERIOD_LAST = "period_last"
CB_PERIOD_RANGE = "period_range"
CB_DAYS_7 = "days_7"
CB_DAYS_14 = "days_14"
CB_DAYS_30 = "days_30"
CB_DAYS_CUSTOM = "days_custom"
CB_HIST_CP = "hist_cp:"
CB_SKIP_TIMES = "skip_times"


class ChartState(StatesGroup):
    """Wizard steps for building a chart from current_queue snapshots."""

    choosing_checkpoint = State()
    choosing_period_type = State()
    entering_days = State()
    entering_dates = State()


def get_main_keyboard() -> ReplyKeyboardMarkup:
    """Persistent reply keyboard with primary bot actions."""
    return ReplyKeyboardMarkup(
        keyboard=[
            [
                KeyboardButton(text="🚗 Current Queue"),
                KeyboardButton(text="📊 Chart"),
            ],
            [
                KeyboardButton(text="📈 7-Day History"),
                KeyboardButton(text="❓ Help"),
            ],
        ],
        resize_keyboard=True,
        input_field_placeholder="Use menu or type a command…",
    )


def get_checkpoints_keyboard(action_prefix: str) -> InlineKeyboardMarkup:
    """
    Build one inline button per checkpoint.

    :param action_prefix: Either CB_CHART_CP or CB_HIST_CP — callback_data = prefix + checkpoint name.
    """
    rows: list[list[InlineKeyboardButton]] = []
    row: list[InlineKeyboardButton] = []
    for cp in DEFAULT_CHECKPOINTS:
        cb = f"{action_prefix}{cp}"
        if len(cb.encode("utf-8")) > 64:
            LOGGER.warning("Callback data too long for checkpoint %r, skipping.", cp)
            continue
        row.append(InlineKeyboardButton(text=cp, callback_data=cb))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    return InlineKeyboardMarkup(inline_keyboard=rows)


def get_period_type_keyboard() -> InlineKeyboardMarkup:
    """Last N days vs custom calendar range."""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Last N days", callback_data=CB_PERIOD_LAST),
                InlineKeyboardButton(text="Custom date range", callback_data=CB_PERIOD_RANGE),
            ],
        ]
    )


def get_preset_days_keyboard() -> InlineKeyboardMarkup:
    """Preset lookback windows plus custom numeric entry."""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="7 days", callback_data=CB_DAYS_7),
                InlineKeyboardButton(text="14 days", callback_data=CB_DAYS_14),
            ],
            [
                InlineKeyboardButton(text="30 days", callback_data=CB_DAYS_30),
                InlineKeyboardButton(text="Custom number…", callback_data=CB_DAYS_CUSTOM),
            ],
        ]
    )


def get_skip_times_keyboard() -> InlineKeyboardMarkup:
    """Optional step: skip time-of-day filter after entering dates."""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="All times (no filter)", callback_data=CB_SKIP_TIMES)],
        ]
    )


_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_TIME_RE = re.compile(r"^\d{1,2}:\d{2}$")


def normalize_hhmm(raw: str) -> str:
    """Normalize '8:00' -> '08:00' for lexicographic comparison in SQLite."""
    parts = raw.strip().split(":")
    if len(parts) != 2:
        raise ValueError(f"Invalid time: {raw!r}")
    h, m = int(parts[0]), int(parts[1])
    if not (0 <= h <= 23 and 0 <= m <= 59):
        raise ValueError(f"Invalid time: {raw!r}")
    return f"{h:02d}:{m:02d}"


def parse_date_range_message(text: str) -> tuple[str, str, str | None, str | None]:
    """
    Parse a single line into (start_date, end_date, time_from, time_to).

    Accepted forms:
    - YYYY-MM-DD YYYY-MM-DD
    - YYYY-MM-DD YYYY-MM-DD HH:MM HH:MM
    """
    tokens = text.split()
    if len(tokens) == 2:
        a, b = tokens
        if _DATE_RE.match(a) and _DATE_RE.match(b):
            return a, b, None, None
    if len(tokens) == 4:
        a, b, tf, tt = tokens
        if _DATE_RE.match(a) and _DATE_RE.match(b) and _TIME_RE.match(tf) and _TIME_RE.match(tt):
            return a, b, normalize_hhmm(tf), normalize_hhmm(tt)
    raise ValueError(
        "Expected:\n`YYYY-MM-DD YYYY-MM-DD`\n"
        "or\n`YYYY-MM-DD YYYY-MM-DD HH:MM HH:MM`\n"
        "Example: `2026-04-01 2026-04-20 08:00 20:00`"
    )


def generate_chart_image(
    rows: list[Any],
    checkpoint: str,
    range_label: str,
    *,
    time_filter_note: str | None = None,
) -> bytes:
    """
    Build a PNG chart from current_queue rows (timestamp, cars, trucks, buses).

    :param rows: sqlite3.Row objects with keys timestamp, cars_out, trucks_out, buses_out.
    :param range_label: Human-readable range description for the title.
    :param time_filter_note: Optional subtitle fragment for time-of-day filter.
    """
    if not rows:
        raise ValueError("No data found for chart.")

    x_values: list[datetime] = []
    cars: list[int] = []
    trucks: list[int] = []
    buses: list[int] = []
    for row in rows:
        ts_raw = row["timestamp"]
        try:
            x_values.append(datetime.fromisoformat(ts_raw))
            cars.append(int(row["cars_out"]))
            trucks.append(int(row["trucks_out"]))
            buses.append(int(row["buses_out"]))
        except ValueError:
            LOGGER.warning("Bad timestamp %r, skipping point.", ts_raw)

    if not x_values:
        raise ValueError("No data found for chart.")

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(x_values, cars, label="Cars", linewidth=2)
    ax.plot(x_values, trucks, label="Trucks", linewidth=2)
    ax.plot(x_values, buses, label="Buses", linewidth=2)

    title = f"Queue trend: {checkpoint}\n{range_label}"
    if time_filter_note:
        title += f"\n{time_filter_note}"
    ax.set_title(title)
    ax.set_xlabel("Time")
    ax.set_ylabel("Queue length")
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d\n%H:%M"))
    ax.legend()
    ax.grid(True, alpha=0.3)
    ax.tick_params(axis="x", rotation=0)
    fig.autofmt_xdate()
    fig.tight_layout()

    buffer = BytesIO()
    fig.savefig(buffer, format="png", dpi=150)
    plt.close(fig)
    buffer.seek(0)
    return buffer.getvalue()


async def generate_and_send_chart(
    message: Message,
    state: FSMContext,
    *,
    checkpoint: str,
    start_date: str,
    end_date: str,
    time_from: str | None,
    time_to: str | None,
    range_label: str,
) -> None:
    """Load DB rows, render chart, send photo, clear FSM, restore reply keyboard."""
    try:
        rows = get_current_queue_range(
            checkpoint=checkpoint,
            start_date=start_date,
            end_date=end_date,
            time_from=time_from,
            time_to=time_to,
        )
        if not rows:
            await message.answer(
                "No data for this period.",
                reply_markup=get_main_keyboard(),
            )
            await state.clear()
            return

        note = None
        if time_from and time_to:
            note = f"Time filter: {time_from}–{time_to}"
        image_bytes = generate_chart_image(
            rows,
            checkpoint,
            range_label,
            time_filter_note=note,
        )
        fname = f"chart_{checkpoint}_{start_date}_{end_date}.png".replace(" ", "_")
        file = BufferedInputFile(image_bytes, filename=fname)
        caption = f"{checkpoint}: {range_label}"
        if note:
            caption += f" ({note})"
        await message.answer_photo(file, caption=caption, reply_markup=get_main_keyboard())
    except Exception:
        LOGGER.exception("Chart generation failed for %s", checkpoint)
        await message.answer(
            "An error occurred while building the chart. Please try again.",
            reply_markup=get_main_keyboard(),
        )
    finally:
        await state.clear()


# --- FSM entry: menu button ---


@chart_router.message(F.text == "📊 Chart")
async def menu_chart_entry(message: Message, state: FSMContext) -> None:
    """Start chart wizard from reply keyboard."""
    await state.set_state(ChartState.choosing_checkpoint)
    await message.answer(
        "Select a checkpoint for the chart:",
        reply_markup=get_checkpoints_keyboard(CB_CHART_CP),
    )


@chart_router.callback_query(
    StateFilter(ChartState.choosing_checkpoint), F.data.startswith(CB_CHART_CP)
)
async def chart_checkpoint_chosen(query: CallbackQuery, state: FSMContext) -> None:
    msg = _callback_host_message(query)
    if msg is None:
        await query.answer("Unsupported message type.", show_alert=True)
        return
    checkpoint = query.data[len(CB_CHART_CP) :]
    await state.update_data(checkpoint=checkpoint)
    await state.set_state(ChartState.choosing_period_type)
    await msg.edit_reply_markup(reply_markup=None)
    await msg.answer(
        f"Checkpoint: **{checkpoint}**\nHow do you want to choose the period?",
        parse_mode="Markdown",
        reply_markup=get_period_type_keyboard(),
    )
    await query.answer()


@chart_router.callback_query(
    StateFilter(ChartState.choosing_period_type), F.data == CB_PERIOD_LAST
)
async def chart_period_last(query: CallbackQuery, state: FSMContext) -> None:
    msg = _callback_host_message(query)
    if msg is None:
        await query.answer("Unsupported message type.", show_alert=True)
        return
    await state.set_state(ChartState.entering_days)
    await msg.edit_reply_markup(reply_markup=None)
    await msg.answer(
        "Pick a preset window or enter a custom number of days in the next message.",
        reply_markup=get_preset_days_keyboard(),
    )
    await query.answer()


@chart_router.callback_query(
    StateFilter(ChartState.choosing_period_type), F.data == CB_PERIOD_RANGE
)
async def chart_period_range(query: CallbackQuery, state: FSMContext) -> None:
    msg = _callback_host_message(query)
    if msg is None:
        await query.answer("Unsupported message type.", show_alert=True)
        return
    await state.set_state(ChartState.entering_dates)
    await msg.edit_reply_markup(reply_markup=None)
    await msg.answer(
        "Send the date range in one line:\n"
        "`YYYY-MM-DD YYYY-MM-DD`\n"
        "Optional time-of-day filter (same line):\n"
        "`YYYY-MM-DD YYYY-MM-DD HH:MM HH:MM`\n\n"
        "Example:\n`2026-04-01 2026-04-20 08:00 20:00`",
        parse_mode="Markdown",
        reply_markup=get_skip_times_keyboard(),
    )
    await query.answer()


@chart_router.callback_query(
    StateFilter(ChartState.entering_days),
    F.data.in_({CB_DAYS_7, CB_DAYS_14, CB_DAYS_30}),
)
async def chart_preset_days(query: CallbackQuery, state: FSMContext) -> None:
    mapping = {CB_DAYS_7: 7, CB_DAYS_14: 14, CB_DAYS_30: 30}
    days = mapping[query.data]
    data = await state.get_data()
    checkpoint = data.get("checkpoint")
    if not checkpoint:
        await state.clear()
        await query.answer("Session expired.", show_alert=True)
        return

    msg = _callback_host_message(query)
    if msg is None:
        await query.answer("Unsupported message type.", show_alert=True)
        return

    end_d = datetime.now().date()
    start_d = end_d - timedelta(days=days)
    start_s = start_d.isoformat()
    end_s = end_d.isoformat()

    await msg.edit_reply_markup(reply_markup=None)
    await generate_and_send_chart(
        msg,
        state,
        checkpoint=checkpoint,
        start_date=start_s,
        end_date=end_s,
        time_from=None,
        time_to=None,
        range_label=f"Last {days} days",
    )
    await query.answer()


@chart_router.callback_query(StateFilter(ChartState.entering_days), F.data == CB_DAYS_CUSTOM)
async def chart_days_custom(query: CallbackQuery, state: FSMContext) -> None:
    msg = _callback_host_message(query)
    if msg is None:
        await query.answer("Unsupported message type.", show_alert=True)
        return
    await msg.edit_reply_markup(reply_markup=None)
    await msg.answer(
        "Enter the number of days (integer, e.g. `21`):",
        parse_mode="Markdown",
    )
    await query.answer()


@chart_router.message(StateFilter(ChartState.entering_days), F.text)
async def chart_custom_days_number(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    if not text.isdigit():
        await message.answer("Please send a positive integer (number of days), or use a preset button.")
        return
    days = int(text)
    if days < 1 or days > 3660:
        await message.answer("Please send a number between 1 and 3660.")
        return
    data = await state.get_data()
    checkpoint = data.get("checkpoint")
    if not checkpoint:
        await state.clear()
        await message.answer("Session expired.", reply_markup=get_main_keyboard())
        return

    end_d = datetime.now().date()
    start_d = end_d - timedelta(days=days)
    await generate_and_send_chart(
        message,
        state,
        checkpoint=checkpoint,
        start_date=start_d.isoformat(),
        end_date=end_d.isoformat(),
        time_from=None,
        time_to=None,
        range_label=f"Last {days} days",
    )


@chart_router.message(StateFilter(ChartState.entering_dates), F.text)
async def chart_dates_text(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    checkpoint = data.get("checkpoint")
    if not checkpoint:
        await state.clear()
        await message.answer("Session expired.", reply_markup=get_main_keyboard())
        return
    try:
        start_s, end_s, tf, tt = parse_date_range_message(message.text.strip())
    except ValueError as exc:
        await message.answer(str(exc))
        return

    if start_s > end_s:
        await message.answer("Start date must be on or before end date.")
        return

    await generate_and_send_chart(
        message,
        state,
        checkpoint=checkpoint,
        start_date=start_s,
        end_date=end_s,
        time_from=tf,
        time_to=tt,
        range_label=f"{start_s} … {end_s}",
    )


@chart_router.callback_query(StateFilter(ChartState.entering_dates), F.data == CB_SKIP_TIMES)
async def chart_skip_times_hint(query: CallbackQuery) -> None:
    """Nudge user: times are optional in the same text as dates."""
    await query.answer(
        "Send dates in one message. Add two times after the dates if you need a day-time window.",
        show_alert=True,
    )


# --- 7-Day History via menu ---


@chart_router.message(F.text == "📈 7-Day History")
async def menu_history_entry(message: Message) -> None:
    await message.answer(
        "Select a checkpoint for 7-day averages (unified daily stats):",
        reply_markup=get_checkpoints_keyboard(CB_HIST_CP),
    )


@chart_router.callback_query(F.data.startswith(CB_HIST_CP))
async def history_checkpoint_chosen(query: CallbackQuery) -> None:
    msg = _callback_host_message(query)
    if msg is None:
        await query.answer("Unsupported message type.", show_alert=True)
        return
    checkpoint = query.data[len(CB_HIST_CP) :]
    rows = get_archive_average(checkpoint=checkpoint, days=7)
    await msg.edit_reply_markup(reply_markup=None)
    if not rows:
        await msg.answer(
            f"No data found for **{checkpoint}** in the last 7 days.",
            parse_mode="Markdown",
            reply_markup=get_main_keyboard(),
        )
        await query.answer()
        return
    lines = [f"Average queue for **{checkpoint}** (last 7 days):"]
    for row in rows:
        lines.append(f"- {row['transport_type']}: {row['avg_queue']}")
    await msg.answer(
        "\n".join(lines),
        parse_mode="Markdown",
        reply_markup=get_main_keyboard(),
    )
    await query.answer()


# --- /chart command extended parsing (delegated from main router) ---


class ChartCliResult:
    """Result of parsing /chart text arguments."""

    __slots__ = ()

    @staticmethod
    def last_n(checkpoint: str, days: int) -> dict[str, Any]:
        return {"mode": "last_n", "checkpoint": checkpoint, "days": days}

    @staticmethod
    def range(
        checkpoint: str,
        start: str,
        end: str,
        time_from: str | None,
        time_to: str | None,
    ) -> dict[str, Any]:
        return {
            "mode": "range",
            "checkpoint": checkpoint,
            "start": start,
            "end": end,
            "time_from": time_from,
            "time_to": time_to,
        }


def parse_slash_chart_args(parts: list[str]) -> dict[str, Any] | None:
    """
    Parse /chart arguments for backward compatibility.

    Supported:
    - <checkpoint> [days]
    - <checkpoint> YYYY-MM-DD YYYY-MM-DD
    - <checkpoint> YYYY-MM-DD YYYY-MM-DD HH:MM HH:MM

    Returns dict for ChartCliResult helpers, or None to start FSM / show usage.
    """
    if not parts:
        return None

    # Range with optional times: last 4 tokens dates+times or last 2 tokens dates
    if len(parts) >= 4:
        a, b, c, d = parts[-4], parts[-3], parts[-2], parts[-1]
        if _DATE_RE.match(a) and _DATE_RE.match(b) and _TIME_RE.match(c) and _TIME_RE.match(d):
            cp = " ".join(parts[:-4]).strip()
            if cp:
                return ChartCliResult.range(
                    cp, a, b, normalize_hhmm(c), normalize_hhmm(d)
                )

    if len(parts) >= 2:
        a, b = parts[-2], parts[-1]
        if _DATE_RE.match(a) and _DATE_RE.match(b):
            cp = " ".join(parts[:-2]).strip()
            if cp:
                return ChartCliResult.range(cp, a, b, None, None)

    days = 7
    rest = parts[:]
    if rest[-1].isdigit():
        days = int(rest[-1])
        rest = rest[:-1]
    cp = " ".join(rest).strip()
    if cp:
        return ChartCliResult.last_n(cp, days)
    return None


async def answer_slash_chart(message: Message, state: FSMContext, parts: list[str]) -> None:
    """Handle /chart with optional text args; empty args start FSM wizard."""
    parsed = parse_slash_chart_args(parts)
    if parsed is None:
        await state.set_state(ChartState.choosing_checkpoint)
        await message.answer(
            "Chart wizard: pick a checkpoint.",
            reply_markup=get_checkpoints_keyboard(CB_CHART_CP),
        )
        return

    if parsed["mode"] == "last_n":
        end_d = datetime.now().date()
        start_d = end_d - timedelta(days=int(parsed["days"]))
        await generate_and_send_chart(
            message,
            state,
            checkpoint=parsed["checkpoint"],
            start_date=start_d.isoformat(),
            end_date=end_d.isoformat(),
            time_from=None,
            time_to=None,
            range_label=f"Last {parsed['days']} days",
        )
        return

    start_s, end_s = parsed["start"], parsed["end"]
    if start_s > end_s:
        await message.answer("Start date must be on or before end date.")
        return
    await generate_and_send_chart(
        message,
        state,
        checkpoint=parsed["checkpoint"],
        start_date=start_s,
        end_date=end_s,
        time_from=parsed["time_from"],
        time_to=parsed["time_to"],
        range_label=f"{start_s} … {end_s}",
    )
