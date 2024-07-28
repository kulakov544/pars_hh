import pandas as pd
import numpy as np
import os
from sqlalchemy import create_engine, Date, Boolean
from typing import Literal

from dotenv import load_dotenv


"""
Создание календаря. Скрипт создает в базе данных таблицу из 3 столбцов. Дата, рабочие дни, праздники
"""
load_dotenv()
conn_string = os.getenv("conn_string")
engine = create_engine(conn_string)


def put_data(df: pd.DataFrame,
             table_name: str,
             schema: str,
             dtype: dict,
             if_exists: Literal["fail", "replace", "append"] = "append") -> bool:
    """Функция, которая загружает дата фрейм в БД, возвращает bool
        :param schema: схема
        :param table_name: название таблицы
        :param if_exists: метод загрузки
        :param df: дата фрейм для записи
        :param dtype: типы данных столбцов
    """
    try:
        conn = engine.connect()
    except Exception as e:
        raise Exception(f'Невозможно установить соединение с сервером: {str(e)}')
    else:
        try:
            df.to_sql(name=table_name, con=conn, schema=schema, if_exists=if_exists, index=False, dtype=dtype)
            return True
        except Exception as e:
            raise e
        finally:
            conn.close()
            engine.dispose()


# Генерируем все даты 2024 года
dates_2024 = pd.date_range(start='2024-01-01', end='2024-12-31')

# Создаем DataFrame
df_calendar = pd.DataFrame(dates_2024, columns=['date'])

# Определяем субботы и воскресенья
df_calendar['work_day'] = df_calendar['date'].dt.dayofweek < 5

# Выходные дни по производственному календарю
prod_calendar_holidays = [
    '2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05',
    '2024-01-06', '2024-01-07', '2024-01-08', '2024-02-23', '2024-03-08',
    '2024-05-01', '2024-05-09', '2024-06-12', '2024-11-04'
]

# Преобразуем строки в даты
prod_calendar_holidays = pd.to_datetime(prod_calendar_holidays)

# Добавляем информацию о выходных и рабочих днях по производственному календарю
df_calendar['holidays'] = df_calendar['date'].isin(prod_calendar_holidays)

# Добавляем информацию о том, является ли день рабочим или выходным по производственному календарю
df_calendar['holidays'] = np.where(df_calendar['date'].isin(prod_calendar_holidays), True, False)

# Определяем типы данных столбцов для загрузки в БД
dtype = {
    'date': Date(),
    'work_day': Boolean(),
    'holidays': Boolean()
}

put_data(df_calendar, table_name='calendar', schema="core", if_exists="append", dtype=dtype)
