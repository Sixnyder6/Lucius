from datetime import datetime

async def get_user_stats(spreadsheet, sheet_name, user_id):
    # Получи все строки таблицы
    all_rows = await get_all_rows_async(spreadsheet, sheet_name)
    total = 0
    today = 0
    today_str = datetime.now().strftime("%Y-%m-%d")
    for row in all_rows:
        # Предположим, что user_id и дата есть в строке
        if row['user_id'] == user_id:
            total += 1
            if row['date'].startswith(today_str):
                today += 1
    return {"total": total, "today": today}