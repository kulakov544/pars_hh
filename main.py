from utilits.get_vacancies_id_utilit import get_vacancies_id
from utilits.get_vacancies_data_utilit import get_vacancies_data
from utilits.connect_database import put_data
from utilits.update_core import update_core
from utilits.logger_utilit import logger


def chunk_list(lst, chunk_size):
    """Разбить список на подсписки фиксированного размера."""
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]

if __name__ == "__main__":
    # Формирование списка параметров
    # ID больших городов России
    big_cities_ids = [3]
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

    if len(vacancies_id_df) > 1:
        # Обработка параметров поиска пакетами по 500 штук
        for search_params_chunk in chunk_list(vacancies_id_df, 500):
            # Сбор данных по вакансиям
            vacancies_data_df, vacancies_skill_df = get_vacancies_data(vacancies_id_df)

            # Загрузка данных в stage
            # Название таблицы
            table_name_data = "stage_pars_hh"
            schema = "stage"
            logger.info('Загрузка данных в stage.')
            put_data(vacancies_data_df, table_name_data, schema, 'replace')

            # Название таблицы
            table_name_skill = "stage_pars_hh_skill"
            schema = "stage"
            put_data(vacancies_skill_df, table_name_skill, schema, 'replace')

            # Обновление core
            logger.info("Перенос данных в core")
            update_core()

            logger.info(f"Собрано {len(vacancies_data_df)} вакансий и сохранено в базу данных")
    else:
        logger.error("Вакансии не найдены")


