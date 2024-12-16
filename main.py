import asyncio
from asyncio import Lock
import asyncpg
from asyncpg import create_pool
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
from dotenv import load_dotenv
import uuid
import hashlib
import ast
import os

import logging

logging.basicConfig(level=logging.DEBUG)  # DEBUG показывает максимум информации

# Загрузка данных из .env
load_dotenv()

db_lock = Lock()

db_pool2 = None


# Асинхронная функция для логирования состояния пула
async def log_pool_state():
    try:
        active_connections = len(db_pool2._holders)  # Занятые соединения
        free_connections = db_pool2._queue.qsize()  # Свободные соединения
        logging.info(
            f"Пул соединений db_pool2: Активных соединений: {active_connections}, Свободных соединений: {free_connections}"
        )
    except Exception as e:
        logging.error(f"Ошибка при логировании состояния пула db_pool2: {e}")


# Асинхронная функция для создания пула подключения
async def get_db_pool2():
    try:
        return await asyncpg.create_pool(
            host=os.getenv("host"),
            port=int(os.getenv("port")),
            user=os.getenv("user"),
            password=os.getenv("password"),
            database=os.getenv("database"),
            max_size=10,  # Укажите максимальный размер пула
        )
        logging.info("Подключение к базе данных успешно")
        return pool
    except Exception as e:
        logging.error(f"Ошибка при подключении к базе данных: {e}")
        raise


BOT_TOKEN = os.getenv("BOT_TOKEN")
GROUP_ID = int(os.getenv("GROUP_ID"))  # Telegram ID администратора

# Инициализация бота и диспетчера
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()


# Регистрация пользователя с созданием нового топика
async def register_user(telegram_id):
    """
    Регистрирует пользователя, создавая анонимный ID и топик для взаимодействия.
    """
    try:
        logging.info(f"Регистрация пользователя с Telegram ID: {telegram_id}")
        async with db_pool2.acquire() as conn:
            # Проверяем, зарегистрирован ли пользователь
            result = await conn.fetchrow(
                "SELECT anon_id, topic_id FROM an_users WHERE telegram_id = $1",
                telegram_id,
            )
            if result:
                logging.info(f"Пользователь {telegram_id} уже зарегистрирован.")
                return result["anon_id"], result["topic_id"]

            # Генерация анонимного ID
            anon_id = str(uuid.uuid4())

            # Создание нового топика
            topic_title = f"Пользователь {anon_id[:8]}"
            topic_result = await bot.create_forum_topic(
                chat_id=GROUP_ID, name=topic_title
            )
            topic_id = topic_result.message_thread_id

            # Сохранение в базе данных
            await conn.execute(
                "INSERT INTO an_users (telegram_id, anon_id, topic_id) VALUES ($1, $2, $3)",
                telegram_id,
                anon_id,
                topic_id,
            )
            logging.info(
                f"Создан новый топик с ID {topic_id} для пользователя {telegram_id}."
            )
            return anon_id, topic_id

    except Exception as e:
        logging.error(f"Ошибка при регистрации пользователя {telegram_id}: {e}")
        return None, None


# Получение Telegram ID по анонимному ID
async def get_telegram_id(anon_id):
    async with db_pool2.acquire() as conn:
        result = await conn.fetchrow(
            "SELECT telegram_id FROM an_users WHERE anon_id = $1", anon_id
        )
        return result["telegram_id"] if result else None


# Обработчик команды /start
@dp.message(Command("start"))
async def start_command(message: types.Message):
    anon_id, topic_id = await register_user(message.from_user.id)
    await message.answer(
        f"Добро пожаловать! Ваш анонимный ID: {anon_id}. Вы можете писать сюда, и администратор ответит."
    )


# Обработчик сообщений от пользователя
@dp.message(F.chat.type == "private")
async def handle_user_message(message: types.Message):
    logging.info(f"Сообщение от {message.from_user.id}: {message.text}")

    # Регистрируем пользователя и получаем данные
    anon_id, topic_id = await register_user(message.from_user.id)

    # Формируем сообщение для отправки в топик
    forward_message = f"Сообщение от {anon_id}:\n{message.text}"

    # Отправляем сообщение в топик
    await bot.send_message(
        chat_id=GROUP_ID, message_thread_id=topic_id, text=forward_message
    )

    # Уведомляем пользователя
    await message.answer("Ваше сообщение отправлено администратору.")


# Обработка ответов администратора в топиках
@dp.message(F.chat.type.in_(["group", "supergroup"]))
async def handle_admin_reply(message: types.Message):
    """
    Обрабатывает сообщения от администратора в топиках группы.
    """
    logging.info(f"Сообщение от администратора: {message.text}, чат: {message.chat.id}")
    try:
        # Проверяем, что сообщение в формате "anon_id: ответ"
        anon_id, reply_text = message.text.split(":", 1)
        anon_id = anon_id.strip()
        reply_text = reply_text.strip()

        # Получаем Telegram ID пользователя по анонимному ID
        telegram_id = await get_telegram_id(anon_id)
        if telegram_id:
            await bot.send_message(
                chat_id=telegram_id, text=f"Ответ от администратора:\n\n{reply_text}"
            )
            logging.info(f"Ответ успешно отправлен пользователю с ID {telegram_id}")
        else:
            await bot.send_message(
                chat_id=message.chat.id,
                text=f"Ошибка: Пользователь с anon_id {anon_id} не найден.",
            )
    except ValueError:
        await message.reply(
            "Неверный формат. Используйте формат: <анонимный ID>: <ответ>"
        )
        logging.error(f"Неверный формат сообщения: {message.text}")
    except Exception as e:
        logging.error(f"Ошибка при обработке ответа администратора: {e}")


# Инициализация пула и запуск бота
async def main():
    global db_pool2
    try:
        db_pool2 = await get_db_pool2()  # Создание пула
        logging.info("Пул db_pool2 успешно создан")

        # Тестовый запрос
        async with db_pool2.acquire() as conn:
            await conn.execute("SELECT 1")
            logging.info("Подключение к базе данных успешно установлено")

        await log_pool_state()  # Логирование состояния пула
        await dp.start_polling(bot)
    except Exception as e:
        logging.error(f"Ошибка при подключении к базе данных: {e}")


if __name__ == "__main__":
    asyncio.run(main())
