import os
import re
import json
import asyncio
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Tuple
import numpy as np
import cv2
import pytesseract
from pyzbar.pyzbar import decode
import easyocr
from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove, InlineKeyboardButton, InlineKeyboardMarkup, InputMediaPhoto
from telegram.constants import ChatAction
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
import nest_asyncio
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ---------------------- CONFIGURATION ----------------------
BOT_TOKEN: str = os.environ.get("BOT_TOKEN", "тут_твой_токен")
TESSERACT_CMD: str = r'C:\Program Files\Tesseract-OCR\tesseract.exe'

creds_json = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON")
if creds_json:
    GOOGLE_CREDENTIALS_PATH = "/app/credentials.json"
    with open(GOOGLE_CREDENTIALS_PATH, "w", encoding="utf-8") as f:
        f.write(creds_json)
else:
    GOOGLE_CREDENTIALS_PATH = r"C:\Users\pankr\PycharmProjects\lucius\credentials\scooteracomulator-1d3a66b4a345.json"
os.environ["GOOGLE_CREDENTIALS_PATH"] = GOOGLE_CREDENTIALS_PATH

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
BUTTON_INFO: str = "ℹ️ Информация"

NOTES_DIR: Path = Path("notes")
TEMP_DIR: Path = Path("temp")

PHOTO_PATHS = [
    (r"C:\Users\pankr\PycharmProjects\lucius\Photos\1 QR.jpg",
     "🟢 <b>QR-код</b>\n\n"
     "Пришли пример такого фото, чтобы добавить <b>уникальный номер самоката</b> — он автоматически попадёт в твою индивидуальную статистику!"),
    (r"C:\Users\pankr\PycharmProjects\lucius\Photos\2 Nomer Zad.jpg",
     "🟡 <b>Задний номер самоката</b>\n\n"
     "Пришли пример такого фото, чтобы добавить <b>уникальный номер самоката</b>, если не доступен QR-код или лень его сканировать."),
    (r"C:\Users\pankr\PycharmProjects\lucius\Photos\Nomer Text.jpg",
     "🔴 <b>Текстовый номер</b>\n\n"
     "Пришли пример такого фото, чтобы добавить <b>уникальный номер самоката</b>, если не доступны оба варианта выше (QR-код и задний номер).")
]

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

def get_user_reply_markup(user_id: int) -> Optional[ReplyKeyboardMarkup]:
    keyboard = [
        [BUTTON_MY_STATS],
        [BUTTON_CONTACT_ADMIN],
        [BUTTON_INFO]
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
                current_datetime: str = datetime.now().strftime("%d.%m. %H:%M")
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
        current_date: str = datetime.now().strftime("%d.%m")

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

        today = datetime.now().strftime("%d.%m")
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

        text = (
            f"👤 Ваша статистика\n\n"
            f"{first_name}\n\n"
            f"За сегодня вы добавили: {len(numbers_today)} самокатов\n"
            f"Дубликатов: {today_duplicates}\n"
            f"Дата последнего добавления: {last_date}\n\n"
            f"Всего добавлено самокатов: {len(all_numbers)}"
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

# ------ EasyOCR integration ------
easyocr_reader = easyocr.Reader(['ru', 'en'])

def extract_number_easyocr(image_path: str) -> Optional[str]:
    result = easyocr_reader.readtext(image_path, detail=0)
    import re
    groups = []
    for line in result:
        m = re.search(r'(\d{4})', line)
        if m:
            groups.append(m.group(1))
    if len(groups) >= 2:
        return groups[0] + groups[1]
    m8 = re.search(r'(\d{8})', ''.join(result))
    if m8:
        return m8.group(1)
    return None

def extract_qr_and_number(image_path: str) -> tuple[Optional[str], Optional[str]]:
    image = cv2.imread(image_path)
    qr_data = None
    decoded = decode(image)
    if decoded:
        qr_data = decoded[0].data.decode("utf-8")
        qr_data = re.sub(r"\D", "", qr_data)  # только цифры

    number = extract_number_easyocr(image_path)
    if not number:
        gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
        ocr_result = pytesseract.image_to_string(gray, config="--psm 6 digits")
        numbers = re.findall(r"\d{4}\s*\d{4}", ocr_result.replace('\n', ''))
        if numbers:
            number = numbers[0].replace(" ", "")

    return qr_data, number

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
        "Доступны кнопки: Моя статистика, Написать админу, Информация.\n"
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
    qr_text, number_text = extract_qr_and_number(file_path)
    spreadsheet = await get_spreadsheet_async()
    if qr_text:
        await append_to_google_sheets_async(spreadsheet, "QR Codes", user_id, [qr_text], context)
        await context.bot.send_message(chat_id=update.message.chat_id, text=f"QR-код {qr_text} сохранён.")
    elif number_text:
        await append_to_google_sheets_async(spreadsheet, "QR Codes", user_id, [number_text], context)
        await context.bot.send_message(chat_id=update.message.chat_id, text=f"Самокат {number_text} сохранён.")
    else:
        await context.bot.send_message(chat_id=update.message.chat_id, text="QR-код или номер не найден.")

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
    await context.bot.send_message(chat_id=update.message.chat_id, text=stats)

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

async def handle_info(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.message.from_user.id
    # Кнопка доступна всем
    media = []
    captions = []
    for path, desc in PHOTO_PATHS:
        if os.path.exists(path):
            media.append(path)
            captions.append(desc)
    if len(media) == 3:
        with open(media[0], "rb") as p1, open(media[1], "rb") as p2, open(media[2], "rb") as p3:
            await context.bot.send_photo(chat_id=update.message.chat_id, photo=p1, caption=captions[0], parse_mode="HTML")
            await context.bot.send_photo(chat_id=update.message.chat_id, photo=p2, caption=captions[1], parse_mode="HTML")
            await context.bot.send_photo(chat_id=update.message.chat_id, photo=p3, caption=captions[2], parse_mode="HTML")
    else:
        await context.bot.send_message(
            chat_id=update.message.chat_id,
            text="Информационные примеры не найдены. Обратитесь к администратору."
        )

# ----------------- MAIN ------------------
async def main() -> None:
    logging.info("Called main function")
    application = Application.builder().token(BOT_TOKEN).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("status", status))
    application.add_handler(MessageHandler(filters.TEXT & filters.Regex(f"^{BUTTON_SAVE_NOTES}$"), save_notes_handler))
    application.add_handler(MessageHandler(filters.TEXT & filters.Regex(f"^{BUTTON_DELETE_NOTE}$"), delete_last_note))
    application.add_handler(MessageHandler(filters.PHOTO & ~filters.COMMAND, handle_photo_with_text))
    application.add_handler(MessageHandler(filters.TEXT & filters.Regex(f"^{BUTTON_VYGRUZKA}$"), handle_vygruzka))
    application.add_handler(MessageHandler(filters.TEXT & filters.Regex(f"^{BUTTON_TABLE}$"), handle_table))
    application.add_handler(MessageHandler(filters.TEXT & filters.Regex(f"^{BUTTON_RETURN}$"), handle_vozvrat))
    application.add_handler(MessageHandler(filters.TEXT & filters.Regex(f"^{BUTTON_MY_STATS}$"), handle_my_stats))
    application.add_handler(MessageHandler(filters.TEXT & filters.Regex(f"^{BUTTON_CONTACT_ADMIN}$"), handle_contact_admin))
    application.add_handler(MessageHandler(filters.TEXT & filters.Regex(f"^{BUTTON_INFO}$"), handle_info))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_message))

    refresh_task = asyncio.create_task(background_refresh())
    await application.run_polling()
    refresh_task.cancel()
    logging.info("Bot is running and polling for updates")

if __name__ == '__main__':
    asyncio.run(main())