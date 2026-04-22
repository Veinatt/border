# Belarus Border Queue Tracker

Проект для мониторинга очередей на границе Беларуси:

- сбор текущей обстановки с `gpk.gov.by` каждые 2 часа;
- разовый сбор исторических данных (архив за 60 дней);
- хранение в SQLite;
- Telegram-бот с командами и графиками;
- ежедневная автоматическая сводка в чат/топик.

## 1. Структура проекта

- `bot/` — код Telegram-бота (aiogram v3).
- `scrapers/` — парсеры текущей страницы и архива.
- `data/` — файл БД `border_queue.db`.
- `utils/` — общие утилиты (пути, логирование).
- `logs/` — файлы логов.
- `scripts/` — `.bat`-скрипты для Windows.

## 2. Требования

- Windows 10/11;
- Python 3.10+;
- Google Chrome (для Selenium в архивном парсере);
- интернет-соединение.

Зависимости зафиксированы в `requirements.txt`.

## 3. Быстрый старт (Windows)

### Вариант A (рекомендуется): автонастройка окружения

Откройте `cmd` в корне проекта и выполните:

```bat
scripts\setup_venv.bat
```

Скрипт:
- создаст `.venv`;
- обновит `pip`;
- установит зависимости;
- покажет версии Python и pip.

### Вариант B: ручная настройка

```bat
py -3.10 -m venv .venv
.venv\Scripts\activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

## 4. Инициализация базы данных

После установки зависимостей:

```bat
.venv\Scripts\activate
python db_manager.py
```

Будет создан файл `data\border_queue.db` с таблицами:

- `current_queue(checkpoint, cars_out, trucks_out, buses_out, timestamp)`;
- `archive_queue(checkpoint, date, transport_type, queue_length, scraped_at)` + защита от дублей через `UNIQUE(checkpoint, date, transport_type)`.

## 5. Как запустить текущий парсер (каждые 2 часа)

### Через скрипт

```bat
scripts\run_scraper.bat
```

### Вручную

```bat
.venv\Scripts\activate
python main_scraper.py
```

Что делает `main_scraper.py`:
- запускает `init_db()`;
- выполняет первый сбор сразу при старте;
- планирует задачу каждые 2 часа через `AsyncIOScheduler`.

## 6. Как запустить архивный парсер (однократно)

```bat
scripts\run_archive_once.bat
```

Или вручную:

```bat
.venv\Scripts\activate
python scrapers/archive_scraper.py
```

Архивный парсер:
- перебирает список КПП;
- проходит по последним 60 дням;
- сохраняет легковые/грузовые/автобусы в `archive_queue`;
- использует `INSERT OR IGNORE`, чтобы не дублировать записи.

## 7. Где настроить парсер

### 7.1 Текущий парсер

Файл: `scrapers/current_scraper.py`

Основные точки настройки:
- `CURRENT_URL` — URL страницы текущей обстановки;
- `DEFAULT_CHECKPOINTS` — список КПП;
- `fetch_current_page_html()` — таймауты, количество retry, backoff;
- `parse_current_queue()` — логика поиска блоков и разбор значений;
- `_extract_transport_values()` — регулярные выражения для чисел по типам транспорта.

### 7.2 Архивный парсер

Файл: `scrapers/archive_scraper.py`

Основные точки настройки:
- `ARCHIVE_URL` — URL страницы архива;
- `CHECKPOINTS` — список КПП;
- `scrape_archive_last_days(days=60, headless=True)` — глубина истории и headless-режим;
- `_select_checkpoint()` — выбор КПП в dropdown;
- `_set_date()` — установка даты в date picker;
- `_parse_archive_table()` — парсинг таблицы.

Если верстка `gpk.gov.by` изменилась, сначала корректируйте именно эти функции.

## 8. Как настроить бота

Файл: `bot/config.py` читает переменные окружения.

### Обязательная переменная

```bat
set TELEGRAM_BOT_TOKEN=123456789:YOUR_REAL_TOKEN
```

### Опциональные (для ежедневной рассылки)

```bat
set TARGET_CHAT_ID=-1001234567890
set TARGET_MESSAGE_THREAD_ID=42
set BOT_TIMEZONE=Europe/Minsk
```

Где используются:
- `TELEGRAM_BOT_TOKEN` — авторизация бота;
- `TARGET_CHAT_ID` — чат для daily summary;
- `TARGET_MESSAGE_THREAD_ID` — топик в группе (если используете темы);
- `BOT_TIMEZONE` — часовой пояс планировщика (по умолчанию `Europe/Minsk`).

## 9. Как запустить бота

После установки токена:

```bat
scripts\run_bot.bat
```

Или вручную:

```bat
.venv\Scripts\activate
python bot/main.py
```

## 10. Как пользоваться ботом

Доступные команды:

- `/start` — справка по командам;
- `/queue` — последняя сохраненная сводка по КПП;
- `/history <КПП>` — средние значения за 7 дней (по архиву);
- `/chart <КПП> [дни]` — график тренда из `current_queue`.

Примеры:

```text
/history Брест
/chart Каменный Лог 14
```

График отправляется как изображение, формируется в памяти (`BytesIO`) без записи на диск.

## 11. Ежедневная автоматическая сводка (09:00)

В `bot/main.py` планировщик `AsyncIOScheduler` запускает отправку daily summary:

- сообщение с топ-3 КПП по легковым;
- мини-график топ-3.

Чтобы это работало, задайте `TARGET_CHAT_ID` (и при необходимости `TARGET_MESSAGE_THREAD_ID`).

## 12. Как получить `chat_id` и `message_thread_id`

1. Добавьте бота в группу/канал (и в нужный топик, если форум-группа).
2. Временно добавьте в `bot/main.py` отладочный хендлер:

```python
@router.message()
async def debug_ids(message: Message) -> None:
    await message.answer(
        f"chat_id={message.chat.id}, thread_id={message.message_thread_id}"
    )
```

3. Перезапустите бота и отправьте сообщение в нужный чат/топик.
4. Скопируйте значения и установите переменные окружения.
5. Удалите временный debug-хендлер.

## 13. Автозапуск при старте Windows (Task Scheduler)

Создайте 2 задачи: отдельно для скрейпера и отдельно для бота.

### Общие шаги

1. Откройте `Task Scheduler` -> `Create Task...`.
2. Вкладка `General`:
   - имя, например `BorderQueueScraper` / `BorderQueueBot`;
   - включите `Run whether user is logged on or not`;
   - включите `Run with highest privileges`.
3. Вкладка `Triggers` -> `New...`:
   - `Begin the task`: `At startup`.
4. Вкладка `Actions` -> `New...`:
   - `Program/script`: `cmd.exe`;
   - `Add arguments`:
     - для скрейпера: `/c "D:\minipets\borders-pet\scripts\run_scraper.bat"`
     - для бота: `/c "D:\minipets\borders-pet\scripts\run_bot.bat"`
5. Сохраните задачи и проверьте кнопкой `Run`.

## 14. Логи и диагностика

Логи:

- `logs/scraper.log`;
- `logs/bot.log`;
- вывод в консоль.

Типовые проблемы:

- сайт временно недоступен — дождитесь следующего цикла или проверьте интернет;
- изменился HTML — обновите селекторы/regex в парсере;
- пустые графики — в БД еще мало данных;
- Selenium-ошибки — обновите Chrome, затем повторите запуск;
- Telegram-ошибки авторизации — проверьте `TELEGRAM_BOT_TOKEN`.

## 15. Как обновлять CSS-селекторы через DevTools

1. Откройте нужную страницу (`current` или `archive`).
2. Нажмите `F12`.
3. Включите выбор элемента (`Ctrl+Shift+C`) и кликните по блоку с очередью.
4. В `Elements` найдите стабильный контейнер строки/таблицы.
5. ПКМ -> `Copy` -> `Copy selector`.
6. Проверьте в консоли:

```js
document.querySelectorAll("ВАШ_СЕЛЕКТОР").length
```

7. Обновите селекторы в:
- `scrapers/current_scraper.py`;
- `scrapers/archive_scraper.py`.

## 16. Полезные команды обслуживания

```bat
python -m compileall .
```

Проверка синтаксиса всех Python-файлов.

```bat
python db_manager.py
```

Повторная инициализация таблиц (без удаления данных).

## 17. Идеи для дальнейшего развития

- переход на PostgreSQL;
- расширенная аналитика (moving average, аномалии, прогнозы);
- Docker для простого деплоя на VPS/Linux;
- веб-панель (например, FastAPI + Plotly);
- алерты при резком росте очередей.
