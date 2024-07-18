from sqlalchemy import create_engine, Column, String, Float, Boolean, DateTime, JSON, MetaData, Table, types


from logger_utilit import logger


@logger.catch()
def create_table(engine, table_name):
    """
    Функция создает таблицу в базе если её нет.
    :param engine: Соединение с базой
    :param table_name: Название таблицы
    :return:
    """
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