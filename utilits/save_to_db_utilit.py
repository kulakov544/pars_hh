from sqlalchemy import create_engine
from pandas import DataFrame

from utilits.logger_utilit import logger
from config import db_url


@logger.catch()
def save_to_db(vacancies_df: DataFrame, table_name: str, schema: str):
    """
    Функция сохраняет вакансии в базу
    :param schema: схема
    :param table_name: Название таблицы
    :param vacancies_df: Список вакансий
    :return:
    """
    try:
        engine = create_engine(db_url)
        conn = engine.connect()
    except Exception as e:
        raise Exception(f'Невозможно установить соединение с сервером: {str(e)}')
    else:
        try:
            vacancies_df.to_sql(table_name, conn, schema=schema, if_exists='replace', index=False)
        except Exception as e:
            raise e
        finally:
            conn.close()
            engine.dispose()
