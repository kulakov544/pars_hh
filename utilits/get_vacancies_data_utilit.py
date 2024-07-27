import requests
import pandas as pd
import time

from pandas import DataFrame


def get_vacancy_details(vacancy_id):
    url = f'https://api.hh.ru/vacancies/{vacancy_id}'
    headers = {
        'Authorization': 'Bearer USERGQ1N9GML4GPQANT3KFSLE160DKD1JN97ODLLIEJ0JAPG99GR1E5OMG8QKQ96'
    }

    response = requests.get(url, headers=headers)
    data = response.json()
    #print(data)

    if response.status_code != 200:
        print(f"Error: Received status code {response.status_code} for vacancy_id {vacancy_id}")
        return None
    return data


def get_vacancies_data(vacancies_id: DataFrame):
    print('Получение данных по вакансиям')

    all_vacancies_data = []
    all_vacancies_skill = []

    for vacancy_id in vacancies_id['vacancy_id']:
        vacancy_data = get_vacancy_details(vacancy_id)
        if vacancy_data:
            # Извлечение нужных данных из ответа
            vacancy_info = {
                'id': vacancy_data.get('id'),
                'name': vacancy_data.get('name'),
                'area': vacancy_data.get('area')['name'],
                'alternate_url': vacancy_data.get('alternate_url'),
                'approved': vacancy_data.get('approved'),
                'archived': vacancy_data.get('archived'),
                'description': vacancy_data.get('description'),
                'driver_license_types': vacancy_data.get('driver_license_types')['id'],
                'employer_name': vacancy_data.get('employer')['name'],
                'employer_url': vacancy_data.get('employer')['alternate_url'],
                'employment': vacancy_data.get('employment')['name'],
                'experience': vacancy_data.get('experience')['name'],
                'has_test': vacancy_data.get('has_test'),
                'initial_created_at': vacancy_data.get('initial_created_at'),
                'languages_name': vacancy_data.get('languages')['level']['name'],
                'premium': vacancy_data.get('premium'),
                'professional_roles': vacancy_data.get('professional_roles')['name'],
                'published_at': vacancy_data.get('published_at'),
                'salary_from': vacancy_data.get('salary')['from'],
                'salary_to': vacancy_data.get('salary')['to'],
                'salary_currency': vacancy_data.get('salary')['currency'],
                'schedule': vacancy_data.get('schedule')['name'],
                'working_days': vacancy_data.get('working_days')['name'],
                'working_time_intervals': vacancy_data.get('working_time_intervals')['name'],
                'working_time_modes': vacancy_data.get('working_time_modes')['name'],
                'address_city': vacancy_data.get('address')['city'],
                'address_street': vacancy_data.get('address')['street'],
                'address_lat': vacancy_data.get('address')['lat'],
                'address_lng': vacancy_data.get('address')['lng'],
            }
            vacancies_skill = {
                'id': vacancy_data.get('id'),
                'key_skills': vacancy_data.get('key_skills')
            }

            all_vacancies_data.append(vacancy_info)
            all_vacancies_skill.append(vacancies_skill)
        time.sleep(0.1)  # Добавьте задержку между запросами

    vacancies_df = pd.DataFrame(all_vacancies_data)
    vacancies_df.drop_duplicates(subset=['id'], inplace=True)

    vacancies_skill_df = pd.DataFrame(all_vacancies_skill)
    vacancies_skill_df.drop_duplicates(subset=['id'], inplace=True)

    #return vacancies_df

    vacancies_df.to_csv('vacancies_data.csv', index=False)  # Сохранение DataFrame в CSV файл
    print("DataFrame saved to vacancies_data.csv")

    vacancies_skill_df.to_csv('vacancies_skill.csv', index=False)  # Сохранение DataFrame в CSV файл
    print("DataFrame saved to vacancies_skill.csv")
