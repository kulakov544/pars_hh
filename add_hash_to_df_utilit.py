import hashlib

from pandas import DataFrame


def add_hash_to_df(vacancy, hash_column_name='vacancy_hash', encoding='utf-8') -> DataFrame:
    """
    Функция генерирует хеш по вакансиям. (Не забыть в нем разобраться, кажется он берет весь df)
    :param vacancy: Датафрейм с вакансиями
    :return: хеш вакансии
    """

    def row_to_hash(row):
        # Преобразуем строку в строковое представление и кодируем в байты
        row_str = str(row.to_dict())
        row_bytes = row_str.encode(encoding)
        # Генерируем хеш
        hash_object = hashlib.sha256(row_bytes)
        return hash_object.hexdigest()

    vacancy[hash_column_name] = vacancy.apply(row_to_hash, axis=1)
    return vacancy
