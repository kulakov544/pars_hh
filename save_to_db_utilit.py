from sqlalchemy import create_engine, Column, String, Float, Boolean, DateTime, JSON, MetaData, Table, types

from logger_utilit import logger
from create_table_utilit import create_table
from config import db_url


@logger.catch()
def save_to_db(vacancies_df):
    """
    Функция сохраняет вакансии в базу
    :param vacancies_df: Список вакансий
    :return:
    """
    # Название таблицы
    table_name = "stage_pars_hh"

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

    try:
        engine = create_engine(db_url)
        conn = engine.connect()
    except Exception as e:
        raise Exception(f'Невозможно установить соединение с сервером: {str(e)}')
    else:
        try:
            vacancies_df.to_sql(table_name, conn, if_exists='replace', index=False, dtype=dtype)
        except Exception as e:
            raise e
        finally:
            conn.close()
            engine.dispose()
