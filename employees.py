import json

def load_employees(file_path='data/employees.json'):
    with open(file_path, encoding='utf-8') as f:
        return json.load(f)

def get_employee_by_id(user_id, employees):
    for emp in employees:
        if emp['id'] == user_id:
            return emp
    return None

# Для теста (можно удалить или оставить для проверки):
if __name__ == "__main__":
    employees = load_employees()
    user_id = 1181905320
    emp = get_employee_by_id(user_id, employees)
    if emp:
        print(f"{emp['first_name']} {emp['last_name']}")
    else:
        print("Сотрудник не найден")