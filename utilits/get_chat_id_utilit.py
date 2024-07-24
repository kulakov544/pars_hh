import requests

from utilits.logger_utilit import logger


def get_chat_id(token: str) -> int:
    # URL для запроса getUpdates
    url = f'https://api.telegram.org/bot{token}/getUpdates'

    try:
        # Отправка запроса
        response = requests.get(url)
        data = response.json()
    except Exception as e:
        logger.error(e)
    else:
        # Получаем CHAT_ID из результата
        if 'result' in data:
            for result in data['result']:
                chat_id = result['message']['chat']['id']
                return chat_id
        else:
            logger.error("No chat_id found")