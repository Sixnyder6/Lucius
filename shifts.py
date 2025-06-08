async def get_user_shifts(spreadsheet, sheet_name, user_id):
    all_rows = await get_all_rows_async(spreadsheet, sheet_name)
    shifts = []
    for row in all_rows:
        if row['user_id'] == user_id:
            shifts.append({"date": row["date"], "shift": row["shift"]})
    return shifts