import os
import re
import json
import asyncio
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from pathlib import Path
from typing import Optional, List, Tuple
import numpy as np
import cv2
import pytesseract
from pyzbar.pyzbar import decode
from PIL import Image
from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ChatAction
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
import nest_asyncio
from openpyxl import Workbook, load_workbook
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ---------------------- TIMEZONE SETUP ----------------------
MOSCOW_TZ = ZoneInfo("Europe/Moscow")
def now_moscow():
    return datetime.now(MOSCOW_TZ)
# -----------------------------------------------------------

# ---------------------- CONFIGURATION ----------------------
BOT_TOKEN: str = os.environ.get("BOT_TOKEN", "тут_твой_токен")
TESSERACT_CMD: str = r'C:\Program Files\Tesseract-OCR\tesseract.exe'

# ------ Вот этот блок должен быть только один раз ------
creds_json = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON")
if creds_json:
    GOOGLE_CREDENTIALS_PATH = "/app/credentials.json"
    with open(GOOGLE_CREDENTIALS_PATH, "w", encoding="utf-8") as f:
        f.write(creds_json)
else:
    # Фолбэк для локального запуска, если переменной окружения нет
    GOOGLE_CREDENTIALS_PATH = r"C:\Users\pankr\PycharmProjects\lucius\credentials\scooteracomulator-1d3a66b4a345.json"

os.environ["GOOGLE_CREDENTIALS_PATH"] = GOOGLE_CREDENTIALS_PATH
# --------------------------------------------------------

GOOGLE_SHEET_URL: str = "https://docs.google.com/spreadsheets/d/1-xD9Yst0XiEmoSMzz1V6IGxzHTtOAJdkxykQLlwhk9Q/edit?usp=sharing"

ALLOWED_USERS: List[int] = [
    1181905320, 5847349753, 6591579113, 447217410,
    6798620038, 803525517, 6477970486, 919223506,
    834962174, 1649277905, 1812295057, 1955102736, 692242823,
    7388938513, 717164010
]
SPECIAL_USER_IDS: Tuple[int, int] = (1181905320, 1955102736)
ADMIN_USER_ID = 1181905320  # Соболев Владислав

BUTTON_VYGRUZKA: str = "📤 Выгрузка"
BUTTON_RETURN: str = "🔙 Вернуться"
BUTTON_SAVE_NOTES: str = "💾 Сохранить заметку"
BUTTON_DELETE_NOTE: str = "❌ Удалить последнюю заметку"
BUTTON_TABLE: str = "📊 Таблица"
BUTTON_MY_STATS: str = "👤 Моя статистика"
BUTTON_CONTACT_ADMIN: str = "📩 Написать админу"
BUTTON_MY_SHIFTS: str = "📅 Мой график"

NOTES_DIR: Path = Path("notes")
TEMP_DIR: Path = Path("temp")
GRAFIK_PATH = Path("grafik.json")
LAST_ACTIVITY_PATH = Path("last_activity.json")

pytesseract.pytesseract.tesseract_cmd = TESSERACT_CMD

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

nest_asyncio.apply()

# -------------------- REGEX & VALIDATION --------------------
NUMBER_PATTERN = re.compile(r'00\d{6}')
def is_valid_number(text: str) -> Optional[str]:
    match = NUMBER_PATTERN.search(text)
    return match.group(0) if match else None

SHIFT_SYMBOLS = {
    "work": "🟢 Рабочий день",
    "closed": "🟡 Закрыто",
    "off": "🔴 Выходной"
}

def get_last_activity(user_id: int) -> str:
    if LAST_ACTIVITY_PATH.exists():
        with open(LAST_ACTIVITY_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get(str(user_id), "")
    return ""

def update_last_activity(user_id: int):
    today = now_moscow().strftime("%Y-%m-%d")
    if LAST_ACTIVITY_PATH.exists():
        with open(LAST_ACTIVITY_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
    else:
        data = {}
    data[str(user_id)] = today
    with open(LAST_ACTIVITY_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f)

def get_user_shift_message(user_id: int, days: int = 15) -> str:
    today = now_moscow().date()
    yesterday = today - timedelta(days=1)
    last_activity = get_last_activity(user_id)

    try:
        with open(GRAFIK_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return "График не найден или повреждён. Обратитесь к администратору."
    user_id_str = str(user_id)
    if user_id_str not in data:
        return "Для вас график пока не назначен."
    shifts = data[user_id_str].get("shifts", {})
    lines = ["🎯 *Ваш персональный график смен*  \n"]
    for i in range(days):
        d = today + timedelta(days=i - 1)  # начинаем с вчерашнего дня
        d_str = d.strftime("%Y-%m-%d")
        d_view = d.strftime("%d %B")
        # если это вчера и пользователь был активен именно вчера — показываем closed
        if d == yesterday and last_activity == d_str:
            shift_type = "closed"
        else:
            shift_type = shifts.get(d_str, "off")
        symbol = SHIFT_SYMBOLS.get(shift_type, "❔ Без данных")
        lines.append(f"📅 {d_view} → {symbol}")
    lines.append("\n➖➖➖➖➖  \n✅ *Обновлено автоматически*  \n")
    return "\n".join(lines)

def get_user_reply_markup(user_id: int) -> Optional[ReplyKeyboardMarkup]:
    keyboard = [
        [BUTTON_MY_STATS],
        [BUTTON_MY_SHIFTS],
        [BUTTON_CONTACT_ADMIN]
    ]
    if user_id in SPECIAL_USER_IDS:
        keyboard.insert(0, [BUTTON_VYGRUZKA])
        keyboard.insert(1, [BUTTON_TABLE])
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

def is_user_allowed(user_id: int) -> bool:
    return user_id in ALLOWED_USERS

def is_duplicate(file_path: Path, new_note: str) -> bool:
    if not file_path.exists():
        return False
    with file_path.open('r', encoding='utf-8') as file:
        existing_notes = json.load(file)
    return new_note in existing_notes

def log_unauthorized_access(user_id: int, action: str):
    logging.warning(f"Unauthorized access attempt: user_id={user_id}, action={action}")

async def notify_admin(context: ContextTypes.DEFAULT_TYPE, text: str):
    try:
        await context.bot.send_message(chat_id=ADMIN_USER_ID, text=f"[ADMIN ALERT]\n{text}")
    except Exception as e:
        logging.error(f"Failed to notify admin: {e}")

# -------------------- ASYNC GOOGLE SHEETS --------------------
def authorize_google_sheets() -> gspread.Client:
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    credentials = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_CREDENTIALS_PATH, scope)
    client = gspread.authorize(credentials)
    return client

async def get_spreadsheet_async() -> gspread.Spreadsheet:
    loop = asyncio.get_running_loop()
    def _func():
        client = authorize_google_sheets()
        spreadsheet = client.open_by_url(GOOGLE_SHEET_URL)
        return spreadsheet
    return await loop.run_in_executor(None, _func)

user_names: dict[int, str] = {
    1181905320: "Соболев Владислав",
    5847349753: "Долгих Олег",
    6591579113: "Пантюхин Игорь",
    447217410: "Пантюхин Сергей",
    6798620038: "Солопов Михаил",
    803525517: "Галкин Аким",
    6477970486: "Дайлиденок Савлелий",
    919223506: "Танасенко Даниил",
    834962174: "Щербаченко Владимир",
    1649277905: "Бойко Илья",
    1812295057: "Соколов Дмитрий",
    717164010: "Литвинов Владислав",
    692242823: "Зленко Александр",
    7388938513: "Саранцев Игорь",
}
user_column_map: dict[str, Tuple[int, int]] = {
    "Соболев Владислав": (1, 2),
    "Долгих Олег": (3, 4),
    "Пантюхин Игорь": (5, 6),
    "Пантюхин Сергей": (7, 8),
    "Солопов Михаил": (9, 10),
    "Галкин Аким": (11, 12),
    "Дайлиденок Савлелий": (13, 14),
    "Танасенко Даниил": (15, 16),
    "Щербаченко Владимир": (17, 18),
    "Бойко Илья": (19, 20),
    "Соколов Дмитрий": (21, 22),
    "Литвинов Владислав": (23, 24),
    "Зленко Александр": (25, 26),
    "Саранцев Игорь": (27, 28),
}

# ------------ SHEETS API LIMITS & RETRIES -------------------
async def append_to_google_sheets_async(spreadsheet: gspread.Spreadsheet, sheet_name: str, user_id: int, data: List[str], context=None) -> None:
    loop = asyncio.get_running_loop()
    max_attempts = 3
    for attempt in range(max_attempts):
        try:
            def _func():
                try:
                    sheet = spreadsheet.worksheet(sheet_name)
                except Exception as e:
                    logging.error(f"Error accessing worksheet {sheet_name}: {e}")
                    return

                user_name: str = user_names.get(user_id, "Unknown User")
                user_columns: Optional[Tuple[int, int]] = user_column_map.get(user_name)
                if not user_columns:
                    logging.error(f"No columns assigned for user: {user_name}")
                    return

                number_column, datetime_column = user_columns
                next_row: int = max(len(sheet.col_values(number_column)) + 1, 2)
                current_datetime: str = now_moscow().strftime("%d.%m. %H:%M")
                existing_numbers: List[str] = sheet.col_values(number_column)[1:]
                if data[0] in existing_numbers:
                    duplicate_row: int = existing_numbers.index(data[0]) + 2
                    requests = [{
                        "repeatCell": {
                            "range": {
                                "sheetId": sheet._properties['sheetId'],
                                "startRowIndex": duplicate_row - 1,
                                "endRowIndex": duplicate_row,
                                "startColumnIndex": number_column - 1,
                                "endColumnIndex": number_column
                            },
                            "cell": {
                                "userEnteredFormat": {
                                    "backgroundColor": {"red": 1, "green": 0, "blue": 0}
                                }
                            },
                            "fields": "userEnteredFormat.backgroundColor"
                        }
                    }, {
                        "repeatCell": {
                            "range": {
                                "sheetId": sheet._properties['sheetId'],
                                "startRowIndex": duplicate_row - 1,
                                "endRowIndex": duplicate_row,
                                "startColumnIndex": datetime_column - 1,
                                "endColumnIndex": datetime_column
                            },
                            "cell": {
                                "userEnteredFormat": {
                                    "backgroundColor": {"red": 1, "green": 0, "blue": 0}
                                }
                            },
                            "fields": "userEnteredFormat.backgroundColor"
                        }
                    }]
                    spreadsheet.batch_update({"requests": requests})
                    logging.info(f"Duplicate scooter found and highlighted: {data[0]} at row {duplicate_row}")

                sheet.update_cell(next_row, number_column, f"'{data[0]}")
                sheet.update_cell(next_row, datetime_column, current_datetime)
                logging.info(f"Data appended to Google Sheets at row {next_row}: {data[0]}, {current_datetime}")
            await loop.run_in_executor(None, _func)
            await asyncio.sleep(1)
            break
        except Exception as e:
            logging.error(f"Google Sheets update error (attempt {attempt+1}): {e}")
            if "429" in str(e) and context:
                await notify_admin(context, f"Google Sheets API rate limit (429) при обновлении. user_id={user_id}, данные={data}")
                await asyncio.sleep(5)
            elif attempt == max_attempts - 1 and context:
                await notify_admin(context, f"Ошибка записи в Google Sheets после {max_attempts} попыток. user_id={user_id}, данные={data}")

async def analyze_google_sheet_data_optimized_async(spreadsheet: gspread.Spreadsheet, sheet_name: str) -> str:
    loop = asyncio.get_running_loop()
    def _func():
        logging.info("Called optimized analyze_google_sheet_data")
        sheet = spreadsheet.worksheet(sheet_name)
        all_data = sheet.get_all_values()
        if not all_data:
            return "Нет данных"

        data_rows = all_data[1:]
        summary_lines: List[str] = []
        overall_total, overall_duplicates = 0, 0
        current_date: str = now_moscow().strftime("%d.%m")

        active_users = []
        for user_name, (col_number, date_col) in user_column_map.items():
            num_idx, date_idx = col_number - 1, date_col - 1
            filtered_numbers, filtered_dates = [], []
            for row in data_rows:
                if len(row) > max(num_idx, date_idx):
                    num = row[num_idx]
                    date_str = row[date_idx]
                    if date_str.strip():
                        try:
                            parsed_date = datetime.strptime(date_str.strip(), "%d.%m. %H:%M")
                            if parsed_date.strftime("%d.%m") == current_date:
                                filtered_numbers.append(num)
                                filtered_dates.append(date_str)
                        except Exception:
                            continue
            total_numbers = len(filtered_numbers)
            duplicate_count = sum(
                count - 1 for count in {n: filtered_numbers.count(n) for n in filtered_numbers}.values() if count > 1)
            last_date = filtered_dates[-1] if filtered_dates else "нет данных"

            if total_numbers > 0:
                active_users.append(user_name)
                summary_lines.append(
                    f"\U0001F7E2 {user_name}\nДата: {last_date}\nВсего самокатов: {total_numbers}\nДубликаты: {duplicate_count}"
                )

            overall_total += total_numbers
            overall_duplicates += duplicate_count

        summary_lines.append(f"\nВсего самокатов: {overall_total}")
        summary_lines.append(f"Всего дубликатов: {overall_duplicates}")
        summary_lines.append(f"Исполнителей: {len(active_users)}")
        return "\n\n".join(summary_lines)
    return await loop.run_in_executor(None, _func)

async def get_personal_stats(spreadsheet: gspread.Spreadsheet, user_id: int) -> str:
    loop = asyncio.get_running_loop()
    def _func():
        sheet = spreadsheet.worksheet("QR Codes")
        all_data = sheet.get_all_values()
        if not all_data or user_id not in user_names:
            return "У вас пока нет добавленных самокатов. Попробуйте отправить номер или QR-код!"

        user_name = user_names[user_id]
        if user_name not in user_column_map:
            return "У вас пока нет добавленных самокатов. Попробуйте отправить номер или QR-код!"

        col_number, date_col = user_column_map[user_name]
        num_idx, date_idx = col_number - 1, date_col - 1

        today = now_moscow().strftime("%d.%m")
        numbers_today = []
        all_numbers = []
        all_dates = []

        for row in all_data[1:]:
            if len(row) > max(num_idx, date_idx):
                number = row[num_idx]
                date_str = row[date_idx]
                if number.strip():
                    all_numbers.append(number)
                    all_dates.append(date_str)
                    try:
                        parsed_date = datetime.strptime(date_str.strip(), "%d.%m. %H:%M")
                        if parsed_date.strftime("%d.%m") == today:
                            numbers_today.append(number)
                    except Exception:
                        continue

        if not all_numbers:
            return "У вас пока нет добавленных самокатов. Попробуйте отправить номер или QR-код!"

        today_duplicates = sum(
            count - 1 for count in {n: numbers_today.count(n) for n in numbers_today}.values() if count > 1)

        last_date = None
        if all_dates and any(all_dates):
            last_dates = [d for d in all_dates if d.strip()]
            last_date = last_dates[-1] if last_dates else "нет данных"
        else:
            last_date = "нет данных"

        first_name = user_name.split()[1] if len(user_name.split()) > 1 else user_name

        # --- Улучшенный вариант ---
        total_for_week = 0
        week_dates = []
        week_numbers = []
        now = now_moscow()
        for row in all_data[1:]:
            if len(row) > max(num_idx, date_idx):
                number = row[num_idx]
                date_str = row[date_idx]
                if number.strip() and date_str.strip():
                    try:
                        dt = datetime.strptime(date_str.strip(), "%d.%m. %H:%M")
                        if (now - dt).days < 7:
                            week_numbers.append(number)
                            week_dates.append(dt)
                    except Exception:
                        continue
        total_for_week = len(week_numbers)

        # Топ день недели
        day_counts = {}
        for dt in week_dates:
            day_str = dt.strftime("%d.%m")
            day_counts[day_str] = day_counts.get(day_str, 0) + 1
        top_day = max(day_counts.items(), key=lambda x: x[1], default=(None, 0))

        # Среднее в день
        unique_days = len(set(dt.strftime("%d.%m") for dt in week_dates))
        avg_per_day = round(total_for_week / unique_days, 2) if unique_days else total_for_week

        # Первый добавленный
        first_added = (
            min([d for d in all_dates if d.strip()], default="нет данных")
            if all_dates and any(all_dates)
            else "нет данных"
        )

        # Ранг пользователя
        user_totals = []
        for uname, (col, datec) in user_column_map.items():
            idx = col - 1
            numbers = []
            for row in all_data[1:]:
                if len(row) > idx:
                    n = row[idx]
                    if n.strip():
                        numbers.append(n)
            user_totals.append((uname, len(numbers)))
        user_totals.sort(key=lambda x: -x[1])
        rank = next((i + 1 for i, v in enumerate(user_totals) if v[0] == user_name), None)

        # Формируем расширенное сообщение
        text = (
            f"👤 <b>Ваша статистика</b>\n\n"
            f"<b>Имя:</b> {first_name}\n\n"
            f"📅 <b>Сегодня:</b>\n"
            f"— Добавлено самокатов: <b>{len(numbers_today)}</b>\n"
            f"— Дубликатов: <b>{today_duplicates}</b>\n"
            f"— Последнее добавление: <b>{last_date}</b>\n\n"
            f"📈 <b>Неделя:</b>\n"
            f"— Самокатов добавлено: <b>{total_for_week}</b>\n"
            f"— Лучший день недели: <b>{top_day[0]} — {top_day[1]} шт.</b>\n"
            f"— Среднее в день: <b>{avg_per_day}</b>\n\n"
            f"📊 <b>Всего:</b>\n"
            f"— Самокатов добавлено: <b>{len(all_numbers)}</b>\n"
            f"— Первый добавленный самокат: <b>{first_added}</b>\n"
            f"🏆 <b>Ранг среди пользователей:</b> <b>{rank} место</b>\n"
        )
        return text
    return await loop.run_in_executor(None, _func)

async def background_refresh() -> None:
    while True:
        try:
            await get_spreadsheet_async()
            await asyncio.sleep(43200)
        except Exception as e:
            logging.error(f"Error during background refresh: {e}")
            await asyncio.sleep(43200)

def rotate_image(image: np.ndarray, angle: float) -> np.ndarray:
    (h, w) = image.shape[:2]
    center = (w / 2, h / 2)
    M = cv2.getRotationMatrix2D(center, angle, 1.0)
    rotated = cv2.warpAffine(image, M, (w, h))
    return rotated

def decode_qr_code(image_path: str) -> Optional[str]:
    logging.info("Called decode_qr_code")
    image = cv2.imread(image_path)
    if image is None:
        logging.error("Failed to load image")
        return None

    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    for angle in [0, 90, 180, 270]:
        rotated_image = rotate_image(gray, angle)
        decoded_objects = decode(rotated_image)
        for obj in decoded_objects:
            qr_text = obj.data.decode("utf-8")
            match = re.search(r'\d{8}', qr_text)
            if match:
                number = match.group(0)
                logging.info(f"Extracted number: {number} at angle {angle}")
                return number

    logging.error("No QR code found at any angle, trying OCR")
    ocr_result = pytesseract.image_to_string(gray)
    match = re.search(r'\d{8}', ocr_result)
    if match:
        number = match.group(0)
        logging.info(f"Extracted number via OCR: {number}")
        return number
    return None

# ------------- HANDLERS -------------

async def save_notes_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_user_allowed(update.message.from_user.id):
        log_unauthorized_access(update.message.from_user.id, "save_notes_handler")
        await context.bot.send_message(chat_id=update.message.chat_id, text="Нет доступа.")
        return
    note = update.message.text
    NOTES_DIR.mkdir(parents=True, exist_ok=True)
    filename = NOTES_DIR / f"notes_{os.getpid()}.json"
    if is_duplicate(filename, note):
        await context.bot.send_message(chat_id=update.message.chat_id, text="Такая заметка уже есть.")
        return
    with filename.open("w", encoding="utf-8") as file:
        json.dump(note, file)
    await context.bot.send_message(chat_id=update.message.chat_id, text=f"Заметка сохранена в файл: {filename}")

async def delete_last_note(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_user_allowed(update.message.from_user.id):
        log_unauthorized_access(update.message.from_user.id, "delete_last_note")
        await context.bot.send_message(chat_id=update.message.chat_id, text="Нет доступа.")
        return

    qr_code_file = NOTES_DIR / "qr_code_data.json"
    if qr_code_file.exists():
        qr_code_file.unlink()
    await context.bot.send_message(chat_id=update.message.chat_id, text="Последняя заметка удалена.")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_user_allowed(update.message.from_user.id):
        log_unauthorized_access(update.message.from_user.id, "start")
        await context.bot.send_message(chat_id=update.message.chat_id, text="Нет доступа.")
        return
    user_id = update.message.from_user.id
    reply_markup = get_user_reply_markup(user_id)
    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text="Привет! Готов к работе.",
        reply_markup=reply_markup if reply_markup else ReplyKeyboardRemove()
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_user_allowed(update.message.from_user.id):
        log_unauthorized_access(update.message.from_user.id, "help_command")
        await context.bot.send_message(chat_id=update.message.chat_id, text="Нет доступа.")
        return
    user_id = update.message.from_user.id
    reply_markup = get_user_reply_markup(user_id)
    text = (
        "Доступные команды:\n"
        "/start — запуск бота\n"
        "/help — помощь\n"
        "/save_notes — сохранить заметку\n"
        "/delete_last_note — удалить последнюю заметку\n"

        "Доступны кнопки: Моя статистика, Мой график, Написать админу.\n"

        "Доступны кнопки: Моя статистика, Написать админу.\n"

        "Кнопки «Выгрузка» и «Таблица» доступны спецпользователям."
    )
    await context.bot.send_message(chat_id=update.message.chat_id, text=text, reply_markup=reply_markup)

async def status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_user_allowed(update.message.from_user.id):
        log_unauthorized_access(update.message.from_user.id, "status")
        await context.bot.send_message(chat_id=update.message.chat_id, text="Бот работает.")
        return
    await context.bot.send_message(chat_id=update.message.chat_id, text="Бот работает.")

async def handle_photo_with_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_user_allowed(update.message.from_user.id):
        log_unauthorized_access(update.message.from_user.id, "handle_photo_with_text")
        await context.bot.send_message(chat_id=update.message.chat_id, text="Нет доступа.")
        return
    user_id = update.message.from_user.id
    photo = update.message.photo[-1]
    file = await photo.get_file()
    file_bytes = await file.download_as_bytearray()
    TEMP_DIR.mkdir(parents=True, exist_ok=True)
    file_path = TEMP_DIR / f"{file.file_id}.jpg"
    with file_path.open('wb') as f:
        f.write(file_bytes)
    await process_qr_photo(update, context, str(file_path), user_id)
    try:
        file_path.unlink()
    except Exception as e:
        logging.error(f"Ошибка удаления временного файла {file_path}: {e}")

async def process_qr_photo(update: Update, context: ContextTypes.DEFAULT_TYPE, file_path: str, user_id: int) -> None:
    await context.bot.send_chat_action(chat_id=update.message.chat_id, action=ChatAction.TYPING)
    qr_text = decode_qr_code(file_path)
    if not qr_text:
        await context.bot.send_message(chat_id=update.message.chat_id, text="QR-код не найден.")
        return
    spreadsheet = await get_spreadsheet_async()
    await append_to_google_sheets_async(spreadsheet, "QR Codes", user_id, [qr_text], context)

    await context.bot.send_message(chat_id=update.message.chat_id, text=f"QR-код {qr_text} сохранён.")

async def handle_text_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_user_allowed(update.message.from_user.id):
        log_unauthorized_access(update.message.from_user.id, "handle_text_message")
        await context.bot.send_message(chat_id=update.message.chat_id, text="Нет доступа.")
        return
    text = update.message.text
    number = is_valid_number(text)
    if number:
        spreadsheet = await get_spreadsheet_async()
        await append_to_google_sheets_async(spreadsheet, "QR Codes", update.message.from_user.id, [number], context)
        update_last_activity(update.message.from_user.id)
        await context.bot.send_message(chat_id=update.message.chat_id, text=f"Самокат {number} сохранён.")
    else:
        await context.bot.send_message(chat_id=update.message.chat_id, text="Самокат не найден.")

async def handle_vygruzka(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.message.from_user.id
    if user_id not in SPECIAL_USER_IDS:
        log_unauthorized_access(user_id, "handle_vygruzka")
        await context.bot.send_message(chat_id=update.message.chat_id, text="Нет доступа к этой функции.")
        return
    try:
        await context.bot.send_chat_action(chat_id=update.message.chat_id, action=ChatAction.TYPING)
        spreadsheet = await get_spreadsheet_async()
        summary = await analyze_google_sheet_data_optimized_async(spreadsheet, "QR Codes")
    except Exception as e:
        logging.error(f"Analysis error: {e}")
        summary = f"Ошибка анализа: {e}"
    reply_markup = ReplyKeyboardMarkup([[BUTTON_RETURN]], resize_keyboard=True)
    await context.bot.send_message(chat_id=update.message.chat_id, text=summary, reply_markup=reply_markup)

async def handle_table(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.message.from_user.id
    if user_id not in SPECIAL_USER_IDS:
        log_unauthorized_access(user_id, "handle_table")
        await context.bot.send_message(chat_id=update.message.chat_id, text="Нет доступа к этой функции.")
        return

    keyboard = [
        [InlineKeyboardButton("Открыть таблицу", url=GOOGLE_SHEET_URL)]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await context.bot.send_message(
        chat_id=update.message.chat_id,
        text="Нажмите кнопку ниже для открытия таблицы:",
        reply_markup=reply_markup
    )

async def handle_vozvrat(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.message.from_user.id
    if user_id not in SPECIAL_USER_IDS:
        log_unauthorized_access(user_id, "handle_vozvrat")
        await context.bot.send_message(chat_id=update.message.chat_id, text="Нет доступа к этой функции.")
        return
    reply_markup = get_user_reply_markup(user_id)
    await context.bot.send_message(chat_id=update.message.chat_id, text="Меню спецпользователя.", reply_markup=reply_markup)

async def handle_my_stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.message.from_user.id
    if not is_user_allowed(user_id):
        log_unauthorized_access(user_id, "handle_my_stats")
        await context.bot.send_message(chat_id=update.message.chat_id, text="Нет доступа к этой функции.")
        return
    await context.bot.send_chat_action(chat_id=update.message.chat_id, action=ChatAction.TYPING)
    spreadsheet = await get_spreadsheet_async()
    stats = await get_personal_stats(spreadsheet, user_id)
    await context.bot.send_message(chat_id=update.message.chat_id, text=stats, parse_mode="HTML")

async def handle_my_shifts(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.message.from_user.id
    message = get_user_shift_message(user_id, days=15)
    await context.bot.send_message(
        chat_id=update.message.chat_id,
        text=message,
        parse_mode="Markdown"
    )

async def handle_contact_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.message.from_user.id
    if not is_user_allowed(user_id):
        log_unauthorized_access(user_id, "handle_contact_admin")
        await context.bot.send_message(chat_id=update.message.chat_id, text="Нет доступа к этой функции.")
        return
    await context.bot.send_message(
        chat_id=update.message.chat_id,
        text="Если возникли вопросы или нужна помощь, напишите админу:\n@Cyberdyne_Industries"
    )

# ----------------- Unit-тесты для функций ------------------
async def test_append_and_duplicate(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if user_id != ADMIN_USER_ID:
        await context.bot.send_message(chat_id=update.message.chat_id, text="Нет доступа к тестам.")
        return
    await context.bot.send_message(chat_id=update.message.chat_id, text="Тест: запись и проверка дубликатов (A/B)...")
    spreadsheet = await get_spreadsheet_async()
    test_number = "00123456"
    await append_to_google_sheets_async(spreadsheet, "QR Codes", user_id, [test_number], context)
    await append_to_google_sheets_async(spreadsheet, "QR Codes", user_id, [test_number], context)
    await context.bot.send_message(chat_id=update.message.chat_id, text="Тест завершён. Проверьте дублирование (см. A/B).")

async def test_qr_decode(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if user_id != ADMIN_USER_ID:
        await context.bot.send_message(chat_id=update.message.chat_id, text="Нет доступа к тестам.")
        return
    await context.bot.send_message(chat_id=update.message.chat_id, text="Отправьте фото для теста декодирования QR.")

# ----------------- MAIN ------------------
async def main() -> None:
    logging.info("Called main function")
    application = Application.builder().token(BOT_TOKEN).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("status", status))
    application.add_handler(CommandHandler("test_append", test_append_and_duplicate))
    application.add_handler(CommandHandler("test_qr", test_qr_decode))
    application.add_handler(MessageHandler(filters.TEXT & filters.Regex(f"^{BUTTON_SAVE_NOTES}$"), save_notes_handler))
    application.add_handler(MessageHandler(filters.TEXT & filters.Regex(f"^{BUTTON_DELETE_NOTE}$"), delete_last_note))
    application.add_handler(MessageHandler(filters.PHOTO & ~filters.COMMAND, handle_photo_with_text))
    application.add_handler(MessageHandler(filters.TEXT & filters.Regex(f"^{BUTTON_VYGRUZKA}$"), handle_vygruzka))
    application.add_handler(MessageHandler(filters.TEXT & filters.Regex(f"^{BUTTON_TABLE}$"), handle_table))
    application.add_handler(MessageHandler(filters.TEXT & filters.Regex(f"^{BUTTON_RETURN}$"), handle_vozvrat))
    application.add_handler(MessageHandler(filters.TEXT & filters.Regex(f"^{BUTTON_MY_STATS}$"), handle_my_stats))
    application.add_handler(MessageHandler(filters.TEXT & filters.Regex(f"^{BUTTON_MY_SHIFTS}$"), handle_my_shifts))
    application.add_handler(MessageHandler(filters.TEXT & filters.Regex(f"^{BUTTON_CONTACT_ADMIN}$"), handle_contact_admin))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_message))

    refresh_task = asyncio.create_task(background_refresh())
    await application.run_polling()
    refresh_task.cancel()
    logging.info("Bot is running and polling for updates")

if __name__ == '__main__':
    asyncio.run(main())