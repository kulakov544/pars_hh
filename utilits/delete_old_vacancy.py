from typing import List
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
import pandas as pd
import requests

from connect_database import put_data, execute_stmt
from utilits.logger_utilit import logger

load_dotenv()
conn_string = os.getenv("conn_string")
engine = create_engine(conn_string)


def get_vacancy_ids() -> List[int]:
    """Получает список vacancy_id из базы данных."""
    query = "SELECT fv.id FROM core.fact_vacancy fv"
    try:
        df = pd.read_sql(query, engine)
        return df['id'].tolist()
    except Exception as e:
        raise Exception(f'Ошибка при получении данных из базы данных: {str(e)}')


def is_vacancy_closed(vacancy_id: int) -> bool:
    """Проверяет, закрыта или удалена ли вакансия на hh.ru.
        :param vacancy_id: ID вакансии.
    """
    url = f"https://api.hh.ru/vacancies/{vacancy_id}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        return data.get('archived', False)
    elif response.status_code == 404:
        return True
    return False


def delete_old_vacancy():
    """
    Функция составляет список id закрытых вакансий и загружает в базу. Потом запускает функцию удаления этих вакансий.
    """
    old_vacancy = []
    vacancy_ids = get_vacancy_ids()
    count = 0
    for vacancy_id in vacancy_ids:
        count += 1
        if count % 100 == 0:
            logger.info(f'Обработано вакансий: {count}/{len(vacancy_ids)}')
            break
        if is_vacancy_closed(vacancy_id):
            old_vacancy.append({"id": vacancy_id})
            logger.info('Найдены устаревшие вакансии: {}', len(old_vacancy))

    old_vacancy_df = pd.DataFrame(old_vacancy)
    put_data(old_vacancy_df, table_name='old_vacancy', schema='stage', if_exists='replace')
    logger.info(f'Всего найдено устаревших вакансий: {len(old_vacancy_df)}')

    sqlt_stmt = "SELECT core.delete_old_vacancy();"
    execute_stmt(sqlt_stmt)
    logger.info("Старые вакансии удалены.")


if __name__ == "__main__":
    delete_old_vacancy()
