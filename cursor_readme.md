# Cursor context: Belarus Border Queue Tracker

Файл для быстрой ориентации агента/разработчика в репозитории. Пользовательский гайд — в `README.md`.

## Зачем проект

Мониторинг очередей на **автодорожных КПП** Беларуси по данным **gpk.gov.by**:

1. **Текущая обстановка** — периодический парсинг HTML (≈ каждые 2 ч), сохранение снимков в SQLite.
2. **Архив** на сайте — разовый/повторный обход дней и КПП через **Selenium + Chrome**, сохранение дневных значений.
3. **Telegram-бот** — очередь, история, графики; опционально ежедневная сводка в чат в 09:00.

## Стек

- Python 3.10+, SQLite (`data/border_queue.db`).
- Парсинг: `requests`, `beautifulsoup4`; архив — `selenium`, `webdriver-manager`.
- Бот: **aiogram 3.x**, FSM с **`MemoryStorage`** (состояние только в памяти процесса).
- Графики: **matplotlib** (`Agg`), ось времени — `matplotlib.dates`.
- Планировщик: **APScheduler** (`main_scraper.py`, daily summary в боте).

## Карта каталогов

| Путь | Назначение |
|------|------------|
| `main_scraper.py` | Цикл: `init_db` → первый сбор → каждые 2 ч `scrape_and_store_current_queue`. |
| `db_manager.py` | SQLite: создание таблиц/миграции, вставки, выборки для бота и unified. |
| `scrapers/current_scraper.py` | URL текущей страницы, `DEFAULT_CHECKPOINTS`, разбор блоков, `insert_current_queue_records`. |
| `scrapers/archive_scraper.py` | Список `CHECKPOINTS`, Selenium, `insert_archive_records`, глубина `scrape_archive_last_days`. |
| `bot/main.py` | Роутер команд `/start`, `/queue`, `/history`, `/chart`, reply-меню, daily summary, подключение `chart_router`. |
| `bot/chart_fsm.py` | FSM графика, inline-клавиатуры, `generate_chart_image`, меню «7-Day History», разбор аргументов `/chart`. |
| `bot/config.py` | `TELEGRAM_BOT_TOKEN`, `TARGET_*`, `BOT_TIMEZONE` из env. |
| `utils/paths.py` | `PROJECT_ROOT`, `DATA_DIR`, `DB_PATH`, `LOGS_DIR`. |
| `utils/logger.py` | `setup_logging(filename)`. |
| `scripts/*.bat` | Windows: venv, скрейпер, архив один раз, бот. |

## База данных (суть)

- **`current_queue`**: каждый снимок — строка `checkpoint`, `cars_out`, `trucks_out`, `buses_out`, `timestamp` (ISO-подобная строка).
- **`archive_queue`**: длинный формат — `(checkpoint, date, transport_type)` → `queue_length`; `UNIQUE` + `INSERT OR IGNORE`.
- **`unified_daily_queue`**: одна строка на `(checkpoint, day)`; поля **`archive_*`** и **`live_*`**; синхронизация при вставках в сырые таблицы.
- **`v_unified_daily_effective`**: view с `effective_* = COALESCE(archive_*, live_*)` — **единая дневная статистика** для `/history` и меню «📈 7-Day History».
- Миграции: **`PRAGMA user_version`** в `db_manager` (`SCHEMA_USER_VERSION`), при апдейте — бэкфилл unified.

### Важные функции `db_manager.py`

- `init_db()` — всё создать/мигрировать.
- `insert_current_queue_records` / `insert_archive_records` — пишут сырые таблицы и **обновляют unified**.
- `get_latest_current_snapshot`, `get_latest_current_timestamp` — последний снимок для `/queue`.
- `get_current_trend(checkpoint, days)` — тренд «с даты now−days» (для обратной совместимости, если где-то ещё используется).
- **`get_current_queue_range(checkpoint, start_date, end_date, time_from?, time_to?)`** — выборка для графиков по календарному диапазону и опционально по **`HH:MM`** в подстроке времени `timestamp`.
- `get_archive_average(checkpoint, days)` — средние по **view** unified.
- `get_daily_top3_from_latest` — топ-3 для daily summary.

## Бот: поведение

- **`/start`**: сброс FSM, длинная справка (Markdown), **`ReplyKeyboardMarkup`**: Current Queue / Chart / 7-Day History / Help.
- **`/queue`** и кнопка 🚗 — `get_latest_current_snapshot`, таблица в `<pre>`.
- **`/history <КПП>`** — `get_archive_average(..., 7)`; кнопка 📈 — выбор КПП inline → те же средние.
- **`/chart`**:
  - без аргументов — как кнопка 📊: FSM (`ChartState` в `chart_fsm.py`);
  - текст: последние N дней; или `дата дата`; или `дата дата HH:MM HH:MM` — см. `parse_slash_chart_args` / `answer_slash_chart`.
- График: **`generate_chart_image`** + **`get_current_queue_range`**; при отсутствии данных — «No data for this period».
- Список КПП в inline для графика/истории берётся из **`DEFAULT_CHECKPOINTS`** в `current_scraper.py` (должен совпадать с тем, что парсится).

### Роутеры aiogram

`Dispatcher(storage=MemoryStorage())`: сначала `include_router(router)` из `main.py`, затем **`include_router(chart_router)`** — у aiogram дочерние роутеры обходятся с учётом порядка; `chart_router` зарегистрирован **последним**, чтобы перехватывать меню/FSM раньше общих хендлеров (проверить при отладке приоритетов).

## Запуск (кратко)

1. `scripts/setup_venv.bat` или venv + `pip install -r requirements.txt`.
2. `python db_manager.py` — создать/обновить БД.
3. Скрейпер: `python main_scraper.py` или `scripts/run_scraper.bat`.
4. Архив (опционально): `python scrapers/archive_scraper.py`.
5. Бот: `set TELEGRAM_BOT_TOKEN=...` → `python bot/main.py` или `scripts/run_bot.bat`.

## Типичные правки

- **Сломался парсинг текущей страницы** — `scrapers/current_scraper.py` (`parse_current_queue`, `_extract_transport_values`, селекторы).
- **Сломался архив** — `archive_scraper.py` (`_select_checkpoint`, `_set_date`, `_parse_archive_table`, Chrome).
- **Новые КПП** — `DEFAULT_CHECKPOINTS` и при необходимости `CHECKPOINTS` в архиве.
- **Новая логика отчётов по дням** — `unified_daily_queue` / view / `get_archive_average`; сырые ряды для внутридневных графиков — **`current_queue`** + **`get_current_queue_range`**.

## Не трогать без причины

- `.env` — по правилам пользователя не читать содержимое; конфиг через переменные окружения (`bot/config.py`).

## Связанные файлы документации

- **`README.md`** — пользовательская полная инструкция (запуск, БД, бот, Task Scheduler, логи).
