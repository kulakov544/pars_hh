from utilits.connect_database import execute_stmt


def update_core():
    # Обновление справочников
    sqlt_stmt = "SELECT core.update_core_ref();"
    execute_stmt(sqlt_stmt)

    # Обновление fact_vacancy
    sqlt_stmt = "SELECT core.update_fact_vacancy();"
    execute_stmt(sqlt_stmt)

