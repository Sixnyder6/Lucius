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
from PIL import Image
from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ChatAction
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
import nest_asyncio
from openpyxl import Workbook, load_workbook
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ---------------------- CONFIGURATION ----------------------
# Корректно: ключ переменной окружения, а не сам токен!
BOT_TOKEN: str = os.environ.get("BOT_TOKEN", "7839713101:AAFcPH9XPx5aZOI52IBbLXKzNwK4QB4F47E")
TESSERACT_CMD: str = r'C:\Program Files\Tesseract-OCR\tesseract.exe'

GOOGLE_CREDENTIALS_PATH: str = r"C:\Users\pankr\PycharmProjects\lucius\credentials\scooteracomulator-92659037b614.json"
os.environ["GOOGLE_CREDENTIALS_PATH"] = GOOGLE_CREDENTIALS_PATH

GOOGLE_SHEET_URL: str = "https://docs.google.com/spreadsheets/d/1-xD9Yst0XiEmoSMzz1V6IGxzHTtOAJdkxykQLlwhk9Q/edit?usp=sharing"

ALLOWED_USERS: List[int] = [
    1181905320, 5847349753, 6591579113, 447217410,
    6798620038, 803525517, 6477970486, 919223506,
    834962174, 1649277905, 1812295057, 1955102736, 692242823,
    7388938513
]
SPECIAL_USER_IDS: Tuple[int, int] = (1181905320, 1955102736)
ADMIN_USER_ID = 1181905320  # Соболев Владислав

BUTTON_VYGRUZKA: str = "📤 Выгрузка"
BUTTON_RETURN: str = "🔙 Вернуться"
BUTTON_SAVE_NOTES: str = "💾 Сохранить заметку"
BUTTON_DELETE_NOTE: str = "❌ Удалить последнюю заметку"
BUTTON_TABLE: str = "📊 Таблица"  # Новая кнопка

NOTES_DIR: Path = Path("notes")
TEMP_DIR: Path = Path("temp")

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
    if user_id in SPECIAL_USER_IDS:
        keyboard = [
            [BUTTON_VYGRUZKA],
            [BUTTON_TABLE]
        ]
        return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    return None

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
                    logging.info(f"Duplicate number found and highlighted: {data[0]} at row {duplicate_row}")

                sheet.update_cell(next_row, number_column, f"'{data[0]}")
                sheet.update_cell(next_row, datetime_column, current_datetime)
                logging.info(f"Data appended to Google Sheets at row {next_row}: {data[0]}, {current_datetime}")
            await loop.run_in_executor(None, _func)
            await asyncio.sleep(1)  # лимитируем массовые операции
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
            overall_total += total_numbers
            duplicate_count = sum(
                count - 1 for count in {n: filtered_numbers.count(n) for n in filtered_numbers}.values() if count > 1)
            overall_duplicates += duplicate_count
            last_date = filtered_dates[-1] if filtered_dates else "нет данных"
            summary_lines.append(
                f"{user_name}\nДата: {last_date}\nВсего: {total_numbers}\nДубликаты: {duplicate_count}"
            )

        summary_lines.append(f"\nВсего номеров: {overall_total}")
        summary_lines.append(f"Всего дубликатов: {overall_duplicates}")
        return "\n\n".join(summary_lines)
    return await loop.run_in_executor(None, _func)

async def background_refresh() -> None:
    while True:
        try:
            await get_spreadsheet_async()
            await asyncio.sleep(43200)  # 12 часов
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

# ------------- REPLIES BELOW MADE SHORT AND IN RUSSIAN -------------

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
    reply_markup = get_user_reply_markup(user_id) if user_id in SPECIAL_USER_IDS else ReplyKeyboardRemove()
    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text="Привет! Готов к работе.",
        reply_markup=reply_markup
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_user_allowed(update.message.from_user.id):
        log_unauthorized_access(update.message.from_user.id, "help_command")
        await context.bot.send_message(chat_id=update.message.chat_id, text="Нет доступа.")
        return
    user_id = update.message.from_user.id
    if user_id in SPECIAL_USER_IDS:
        text = (
            "Доступные команды:\n"
            "/start — запуск бота\n"
            "/help — помощь\n"
            "/save_notes — сохранить заметку\n"
            "/delete_last_note — удалить последнюю заметку\n"
            "Кнопка «Выгрузка» и «Таблица» доступны спецпользователям."
        )
        reply_markup = get_user_reply_markup(user_id)
    else:
        text = (
            "Доступные команды:\n"
            "/start — запуск бота\n"
            "/help — помощь"
        )
        reply_markup = ReplyKeyboardRemove()
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
    # удаление временного файла после обработки
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
        await context.bot.send_message(chat_id=update.message.chat_id, text=f"Номер {number} сохранён.")
    else:
        await context.bot.send_message(chat_id=update.message.chat_id, text="Номер не найден.")

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

# ----------------- Unit-тесты для функций ------------------
async def test_append_and_duplicate(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Тест для Соболева Владислава: пишет в ячейки A/B и проверяет дублирование."""
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
    """Тест обработки QR (только для Владислава)."""
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
    application.add_handler(MessageHandler(filters.TEXT & filters.Regex(BUTTON_SAVE_NOTES), save_notes_handler))
    application.add_handler(MessageHandler(filters.TEXT & filters.Regex(BUTTON_DELETE_NOTE), delete_last_note))
    application.add_handler(MessageHandler(filters.PHOTO & ~filters.COMMAND, handle_photo_with_text))
    application.add_handler(MessageHandler(filters.TEXT & filters.Regex(BUTTON_VYGRUZKA), handle_vygruzka))
    application.add_handler(MessageHandler(filters.TEXT & filters.Regex(BUTTON_TABLE), handle_table))  # Новый handler!
    application.add_handler(MessageHandler(filters.TEXT & filters.Regex(BUTTON_RETURN), handle_vozvrat))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_message))

    refresh_task = asyncio.create_task(background_refresh())
    await application.run_polling()
    refresh_task.cancel()
    logging.info("Bot is running and polling for updates")

if __name__ == '__main__':
    asyncio.run(main())