import hashlib
import json


def generate_hash(vacancy: dict) -> str:
    """
    Функция генерирует хеш по вакансиям. (Не забыть в нем разобраться, кажется он берет весь df)
    :param vacancy: Список вакансий
    :return: хеш вакансии
    """
    hash_string = json.dumps(vacancy, sort_keys=True)
    return hashlib.sha256(hash_string.encode('utf-8')).hexdigest()