import json
import pandas as pd
import requests
import hashlib
import time
from sqlalchemy import create_engine, Column, String, Float, Boolean, DateTime, JSON, MetaData, Table, types
from sqlalchemy.exc import ProgrammingError

from logger_utilit import logger


@logger.catch()
def get_vacancies(params):
    url = "https://api.hh.ru/vacancies"
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
        time.sleep(1)  # Задержка между страницами
    logger.info(f"Собрано {len(all_vacancies)} вакансий и сохранено в базу данных")

    return all_vacancies


def save_to_file(vacancies, filename="vacancies.json"):
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(vacancies, f, ensure_ascii=False, indent=4)


def generate_hash(vacancy):
    hash_string = json.dumps(vacancy, sort_keys=True)
    return hashlib.sha256(hash_string.encode('utf-8')).hexdigest()


@logger.catch()
def create_table(engine, table_name):
    meta = MetaData()
    vacancies = Table(
        table_name, meta,
        Column('vacancy_id', String, primary_key=True),
        Column('premium', Boolean),
        Column('name', String),
        Column('department', String),
        Column('has_test', Boolean),
        Column('response_letter_required', Boolean),
        Column('area', String),
        Column('salary_from', Float),
        Column('salary_to', Float),
        Column('type', String),
        Column('response_url', String),
        Column('sort_point_distance', String),
        Column('published_at', String),
        Column('created_at', String),
        Column('archived', Boolean),
        Column('apply_alternate_url', String),
        Column('show_logo_in_search', Boolean),
        Column('url', String),
        Column('alternate_url', String),
        Column('employer_name', String),
        Column('employer_url', String),
        Column('snippet_requirement', String),
        Column('snippet_responsibility', String),
        Column('contacts', String),
        Column('schedule', String),
        Column('working_days', String),
        Column('working_time_intervals', String),
        Column('working_time_modes', String),
        Column('accept_temporary', Boolean),
        Column('professional_roles', String),
        Column('accept_incomplete_resumes', Boolean),
        Column('experience', String),
        Column('employment', String),
        Column('vacancy_hash', String)
    )
    meta.create_all(engine)


@logger.catch()
def save_to_db(vacancies, db_url, table_name):

    engine = create_engine(db_url)

    # Создание DataFrame из списка вакансий
    vacancies_df = pd.DataFrame([{
        'vacancy_id': v.get('id'),
        'premium': v.get('premium'),
        'name': v.get('name'),
        'department': v.get('department'),
        'has_test': v.get('has_test'),
        'response_letter_required': v.get('response_letter_required'),
        'area': v.get('area')['name'],
        'salary_from': v.get('salary')['from'] if v.get('salary') is not None else None,
        'salary_to': v.get('salary')["to"] if v.get('salary') is not None else None,
        'type': v.get('type')['name'],
        'response_url': v.get('response_url'),
        'sort_point_distance': v.get('sort_point_distance'),
        'published_at': v.get('published_at'),
        'created_at': v.get('created_at'),
        'archived': v.get('archived'),
        'apply_alternate_url': v.get('apply_alternate_url'),
        'show_logo_in_search': v.get('show_logo_in_search'),
        'url': v.get('url'),
        'alternate_url': v.get('alternate_url'),
        'employer_name': v.get('employer')["name"],
        'employer_url': v.get('employer')["alternate_url"],
        'snippet_requirement': v.get('snippet')["requirement"],
        'snippet_responsibility': v.get('snippet')["responsibility"],
        'contacts': v.get('contacts'),
        'schedule': v.get('schedule')["name"],
        "working_days": v.get('working_days')[0]["name"] if v.get('working_days') and len(v.get('working_days')) > 0 else None,
        "working_time_intervals": v.get('working_time_intervals')[0]["name"] if v.get('working_time_intervals') and len(v.get('working_time_intervals')) > 0 else None,
        "working_time_modes": v.get('working_time_modes')[0]["name"] if v.get('working_time_modes') and len(v.get('working_time_modes')) > 0 else None,
        'accept_temporary': v.get('accept_temporary'),
        'professional_roles': v.get('professional_roles')[0]["name"],
        'accept_incomplete_resumes': v.get('accept_incomplete_resumes'),
        'experience': v.get('experience')["name"],
        'employment': v.get('employment')["name"],
        'vacancy_hash': generate_hash(v)
    } for v in vacancies])

    print(vacancies_df)


    dtype = {
        'vacancy_id': String,
        'premium': Boolean,
        'name': String,
        'department': String,
        'has_test': Boolean,
        'response_letter_required': Boolean,
        'area': String,
        'salary_from': Float,
        'salary_to': Float,
        'type': String,
        'response_url': String,
        'sort_point_distance': String,
        'published_at': String,
        'created_at': String,
        'archived': Boolean,
        'apply_alternate_url': String,
        'show_logo_in_search': Boolean,
        'url': String,
        'alternate_url': String,
        'employer_name': String,
        'employer_url': String,
        'snippet_requirement': String,
        'snippet_responsibility': String,
        'contacts': String,
        'schedule': String,
        'working_days': String,
        'working_time_intervals': String,
        'working_time_modes': String,
        'accept_temporary': Boolean,
        'professional_roles': String,
        'accept_incomplete_resumes': Boolean,
        'experience': String,
        'employment': String,
        'vacancy_hash': String,
    }

    vacancies_df.to_sql(table_name, engine, if_exists='append', index=False, dtype=dtype)


    # # Создание DataFrame из списка вакансий
    # vacancies_df = pd.DataFrame([{
    #     'vacancy_id': v['id'],
    #     'premium': v.get('premium'),
    #     'name': v.get('name'),
    #     'department': v.get('department'),
    #     'has_test': v.get('has_test'),
    #     'response_letter_required': v.get('response_letter_required'),
    #     'area': v.get('area')['name'],
    #     'salary_from': v.get('salary')['from'] if v.get('salary') is not None else None,
    #     'salary_to': v.get('salary')["to"] if v.get('salary') is not None else None,
    #     'type': v.get('type')['name'],
    #
    #     'response_url': v.get('response_url'),
    #     'sort_point_distance': v.get('sort_point_distance'),
    #     'published_at': v.get('published_at'),
    #     'created_at': v.get('created_at'),
    #     'archived': v.get('archived'),
    #     'apply_alternate_url': v.get('apply_alternate_url'),
    #
    #     'show_logo_in_search': v.get('show_logo_in_search'),
    #
    #     'url': v.get('url'),
    #     'alternate_url': v.get('alternate_url'),
    #
    #     'employer_name': v.get('employer')["name"],
    #     'employer_url': v.get('employer')["alternate_url"],
    #     'snippet_requirement': v.get('snippet')["requirement"],
    #     'snippet_responsibility': v.get('snippet')["responsibility"],
    #     'contacts': v.get('contacts'),
    #     'schedule': v.get('schedule')["name"],
    #
    #     "working_days": v.get('working_days')[0]["name"] if v.get('working_days') and len(v.get('working_days')) > 0 else None,
    #     "working_time_intervals": v.get('working_time_intervals')[0]["name"] if v.get('working_time_intervals') and len(v.get('working_time_intervals')) > 0 else None,
    #     "working_time_modes": v.get('working_time_modes')[0]["name"] if v.get('working_time_modes') and len(v.get('working_time_modes')) > 0 else None,
    #     'accept_temporary': v.get('accept_temporary'),
    #     'professional_roles': v.get('professional_roles')[0]["name"],
    #     'accept_incomplete_resumes': v.get('accept_incomplete_resumes'),
    #     'experience': v.get('experience')["name"],
    #     'employment': v.get('employment')["name"],
    #     'vacancy_hash': generate_hash(v)
    # } for v in vacancies])

    #
    # # Создание подключения к базе данных
    # engine = create_engine(db_url)
    #
    # # Создание таблицы, если она не существует
    # create_table(engine, table_name)
    #
    #
    # # Сохранение данных в базу данных
    # vacancies_df.to_sql(table_name, engine, if_exists='append', index=False)


if __name__ == "__main__":
    # ID больших городов России
    big_cities_ids = [2, 3]

    # Список параметров поиска
    search_params_list = [
        {"text": "программист python", "area": city_id, "schedule": "remote", "per_page": 100, "page": 0}
        for city_id in big_cities_ids
    ]

    # Параметры подключения к базе данных
    db_url = "postgresql+psycopg2://user544:ROA_Othgc3@pgvps.kulakovav.ru:5432/pars_hh"

    # Название таблицы
    table_name = "stage_pars_hh"

    all_vacancies = []

    for params in search_params_list:
        vacancies = get_vacancies(params)

        save_to_file(vacancies)
        all_vacancies.extend(vacancies)
        time.sleep(3)  # Задержка между получением данных по разным параметрам

    save_to_file(all_vacancies)
    save_to_db(all_vacancies, db_url, table_name)
    logger.info(f"Собрано {len(all_vacancies)} вакансий и сохранено в базу данных")

