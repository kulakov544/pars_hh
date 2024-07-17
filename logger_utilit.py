from loguru import logger

logger.add("logs/app_logs.json", format="{time} {level} {message}", level="DEBUG", rotation="8:00",
               compression="zip", serialize=True)
__all__ = ["logger"]