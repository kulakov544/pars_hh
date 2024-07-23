from loguru import logger
import telebot
import os
from dotenv import load_dotenv

from utilits.get_chat_id_utilit import get_chat_id


load_dotenv()
TOKEN = os.getenv("TOKEN")


class TelegramBotHandler:
    def __init__(self, token, chat_id):
        self.bot = telebot.TeleBot(token)
        self.chat_id = chat_id

    def send_message(self, message):
        try:
            self.bot.send_message(self.chat_id, message)
        except Exception as e:
            print(f"Не удалось отправить сообщение в Telegram: {e}")


# Конфигурация
CHAT_ID = get_chat_id(TOKEN)

telegram_handler = TelegramBotHandler(TOKEN, CHAT_ID)

# Настройка loguru для использования кастомного обработчика
logger.add(lambda msg: telegram_handler.send_message(msg), level="ERROR", format="{time} - {level} - {message}")
