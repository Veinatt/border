import os
from dataclasses import dataclass


@dataclass(slots=True)
class BotConfig:
    token: str
    target_chat_id: int | None
    target_message_thread_id: int | None
    timezone: str


def _parse_optional_int(value: str | None) -> int | None:
    if value is None or value.strip() == "":
        return None
    return int(value)


def load_config() -> BotConfig:
    """
    Load bot config from environment variables.
    Required:
      - TELEGRAM_BOT_TOKEN
    Optional:
      - TARGET_CHAT_ID
      - TARGET_MESSAGE_THREAD_ID
      - BOT_TIMEZONE (default: Europe/Minsk)
    """
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    if not token:
        raise RuntimeError(
            "TELEGRAM_BOT_TOKEN is not set. "
            "Set environment variable before running bot."
        )

    target_chat_id = _parse_optional_int(os.getenv("TARGET_CHAT_ID"))
    target_message_thread_id = _parse_optional_int(os.getenv("TARGET_MESSAGE_THREAD_ID"))
    timezone = os.getenv("BOT_TIMEZONE", "Europe/Minsk").strip() or "Europe/Minsk"

    return BotConfig(
        token=token,
        target_chat_id=target_chat_id,
        target_message_thread_id=target_message_thread_id,
        timezone=timezone,
    )
