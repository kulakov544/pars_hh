import time
import pandas as pd
import requests
from pandas import DataFrame

from logger_utilit import logger
from add_hash_to_df_utilit import add_hash_to_df


@logger.catch()
def get_vacancies(all_params: dict) -> DataFrame:
    """
    Функция получает json с данными о списке вакансий
    :param all_params: params - список параметров для запроса
    :return: all_vacancies - df с данными о списке вакансий
    """
    url = "https://api.hh.ru/vacancies"
    all_vacancies_df = pd.DataFrame()

    for params in all_params:
        while True:
            response = requests.get(url, params=params)
            data = response.json()

            if "items" not in data:
                break

            # Создание DataFrame из списка вакансий
            vacancies_df = pd.DataFrame([{
                'vacancy_id': str(v.get('id')),
                'premium': bool(v.get('premium')),
                'name': str(v.get('name')),
                'department': str(v.get('department')["name"] if v.get('department') is not None else None),
                'has_test': bool(v.get('has_test')),
                'response_letter_required': bool(v.get('response_letter_required')),
                'area': str(v.get('area')['name']),
                'salary_from': v.get('salary')['from'] if v.get('salary') is not None else 0,
                'salary_to': v.get('salary')["to"] if v.get('salary') is not None else 0,
                'type': str(v.get('type')['name']),
                'response_url': str(v.get('response_url')),
                'sort_point_distance': str(v.get('sort_point_distance')),
                'published_at': str(v.get('published_at')),
                'created_at': str(v.get('created_at')),
                'archived': bool(v.get('archived')),
                'apply_alternate_url': str(v.get('apply_alternate_url')),
                'show_logo_in_search': bool(v.get('show_logo_in_search')),
                'url': str(v.get('url')),
                'alternate_url': str(v.get('alternate_url')),
                'employer_name': str(v.get('employer')["name"]),
                'employer_url': str(v.get('employer')["alternate_url"]),
                'snippet_requirement': str(v.get('snippet')["requirement"]),
                'snippet_responsibility': str(v.get('snippet')["responsibility"]),
                'contacts': str(v.get('contacts')),
                'schedule': str(v.get('schedule')["name"]),
                "working_days": str(v.get('working_days')[0]["name"] if v.get('working_days') and len(
                    v.get('working_days')) > 0 else None),
                "working_time_intervals": str(v.get('working_time_intervals')[0]["name"] if v.get(
                    'working_time_intervals') and len(
                    v.get('working_time_intervals')) > 0 else None),
                "working_time_modes": str(v.get('working_time_modes')[0]["name"] if v.get('working_time_modes') and len(
                    v.get('working_time_modes')) > 0 else None),
                'accept_temporary': bool(v.get('accept_temporary')),
                'professional_roles': str(v.get('professional_roles')[0]["name"]),
                'accept_incomplete_resumes': bool(v.get('accept_incomplete_resumes')),
                'experience': str(v.get('experience')["name"]),
                'employment': str(v.get('employment')["name"]),
            } for v in data["items"]])

            all_vacancies_df = pd.concat([all_vacancies_df, vacancies_df], ignore_index=True)


            if params["page"] >= data["pages"] - 1:
                break

            params["page"] += 1
            time.sleep(1)  # Задержка между страницами

        time.sleep(3)  # Задержка между наборами параметров

        all_vacancies_df.drop_duplicates(subset=['vacancy_id'], inplace=True)
        logger.info(f"Собрано {len(all_vacancies_df)} вакансий")

        all_vacancies_df = add_hash_to_df(all_vacancies_df)




    return all_vacancies_df

