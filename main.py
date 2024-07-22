from utilits.logger_utilit import logger
from utilits.get_vacancies_utilit import get_vacancies
from utilits.save_to_db_utilit import save_to_db


if __name__ == "__main__":
    # ID больших городов России
    big_cities_ids = [2, 3, 4]

    # Список параметров поиска
    search_params_list = [
        {"text": "программист python", "area": city_id, "per_page": 100, "page": 0}
        for city_id in big_cities_ids
    ]

    vacancies_df = get_vacancies(search_params_list)

    # Название таблицы
    table_name = "stage_pars_hh"
    schema = "stage"
    save_to_db(vacancies_df, table_name, schema)

    logger.info(f"Собрано {len(vacancies_df)} вакансий и сохранено в базу данных")
