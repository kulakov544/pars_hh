from utilits.get_vacancies_utilit import get_vacancies
from utilits.connect_database import put_data
from utilits.update_core import update_core
from utilits.logger_utilit import logger

#from utilits.log_to_telebot_utilit import logger    #при подключении отправляет логи с ошибками в тг pars_hh544_bot


if __name__ == "__main__":
    # ID больших городов России
    big_cities_ids = [5, 6]
    text_search = ['python']

    # Список параметров поиска
    search_params_list = []

    text_search_length = len(text_search)

    for i, city_id in enumerate(big_cities_ids):
        search_text = text_search[i % text_search_length]
        search_params_list.append(
            {"text": search_text, "area": city_id, "per_page": 100, "page": 0}
        )

    # Создание датафрейма
    logger.info('Начало сбора вакансий')
    vacancies_df = get_vacancies(search_params_list)
    logger.info(f"Всего собрано {len(vacancies_df)} вакансий")

    # Название таблицы
    table_name = "stage_pars_hh"
    schema = "stage"

    # Загрузка данных в stage
    logger.info('Загрузка данных в stage.')
    put_data(vacancies_df, table_name, schema, 'replace')

    # Обновление core
    logger.info("Перенос данных в core")
    update_core()

    logger.info(f"Собрано {len(vacancies_df)} вакансий и сохранено в базу данных")



