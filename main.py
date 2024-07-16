import requests
import json
import psycopg2


def get_python_vacancies():
    url = "https://api.hh.ru/vacancies"
    params = {
        "text": "программист python",  # Поисковый запрос
        "area": 113,  # ID региона Россия
        "schedule": "remote",  # Удаленная работа
        "per_page": 100,  # Количество результатов на страницу
        "page": 0  # Номер страницы
    }

    all_vacancies = []

    while True:
        response = requests.get(url, params=params)
        data = response.json()

        if "items" not in data:
            break

        all_vacancies.extend(data["items"])

        if params["page"] >= data["pages"] - 1:
            break

        params["page"] += 1

    return all_vacancies


def save_to_file(vacancies, filename="vacancies.json"):
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(vacancies, f, ensure_ascii=False, indent=4)


def save_to_db(vacancies):
    # Подключение к базе данных
    conn = psycopg2.connect(
        dbname="your_db_name",
        user="your_db_user",
        password="your_db_password",
        host="your_db_host",
        port="your_db_port"
    )
    cursor = conn.cursor()

    # Создание таблицы если она не существует
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS vacancies (
            id SERIAL PRIMARY KEY,
            vacancy_id VARCHAR(50),
            name VARCHAR(255),
            area_name VARCHAR(255),
            employer_name VARCHAR(255),
            salary_from INT,
            salary_to INT,
            salary_currency VARCHAR(10),
            url TEXT
        )
    """)

    # Вставка данных в таблицу
    for vacancy in vacancies:
        salary_from = vacancy['salary']['from'] if vacancy['salary'] else None
        salary_to = vacancy['salary']['to'] if vacancy['salary'] else None
        salary_currency = vacancy['salary']['currency'] if vacancy['salary'] else None
        cursor.execute("""
            INSERT INTO vacancies (
                vacancy_id, name, area_name, employer_name, salary_from, salary_to, salary_currency, url
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (vacancy_id) DO NOTHING
        """, (
            vacancy['id'],
            vacancy['name'],
            vacancy['area']['name'],
            vacancy['employer']['name'],
            salary_from,
            salary_to,
            salary_currency,
            vacancy['alternate_url']
        ))

    # Сохранение изменений и закрытие соединения
    conn.commit()
    cursor.close()
    conn.close()


if __name__ == "__main__":
    vacancies = get_python_vacancies()
    save_to_file(vacancies)
    save_to_db(vacancies)
    print(f"Собрано {len(vacancies)} вакансий и сохранено в базу данных")
