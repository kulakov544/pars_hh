from utilits.logger_utilit import logger
from utilits.get_vacancies_utilit import get_vacancies
from utilits.save_to_db_utilit import save_to_db


if __name__ == "__main__":
    # ID больших городов России
    big_cities_ids = [1, 2, 3]

    # Список параметров поиска
    search_params_list = [
        {"text": "программист python", "area": city_id, "schedule": "remote", "per_page": 100, "page": 0}
        for city_id in big_cities_ids
    ]

    vacancies_df = get_vacancies(search_params_list)

    save_to_db(vacancies_df)

    logger.info(f"Собрано {len(vacancies_df)} вакансий и сохранено в базу данных")
