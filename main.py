from utilits.get_vacancies_id_utilit import get_vacancies_id
from utilits.get_vacancies_data_utilit import get_vacancies_data
from utilits.connect_database import put_data
from utilits.update_core import update_core
from utilits.logger_utilit import logger

#from utilits.log_to_telebot_utilit import logger    #при подключении отправляет логи с ошибками в тг pars_hh544_bot



if __name__ == "__main__":
    # Формирование списка параметров
    # ID больших городов России
    big_cities_ids = [4]
    text_search = ['python']
    specialization = [1, 2]

    # Список параметров поиска
    search_params_list = []

    for city_id in big_cities_ids:
        for search_text in text_search:
            for specializ in specialization:
                search_params_list.append(
                    {"text": search_text, "area": city_id, "per_page": 100, "page": 0, 'specialization': specializ}
                )

    # Сбор id вакансий
    logger.info('Начало сбора id вакансий')
    vacancies_id_df = get_vacancies_id(search_params_list)
    logger.info(f"Всего собрано {len(vacancies_id_df)} id вакансий")

    print(vacancies_id_df)

    # Сбор данных по вакансиям
    vacancies_data_df = get_vacancies_data(vacancies_id_df)

    # Название таблицы
    table_name = "stage_pars_hh"
    schema = "stage"
"""
    # Загрузка данных в stage
    logger.info('Загрузка данных в stage.')
    put_data(vacancies_df, table_name, schema, 'replace')

    # Обновление core
    logger.info("Перенос данных в core")
    update_core()

    logger.info(f"Собрано {len(vacancies_df)} вакансий и сохранено в базу данных")

"""

