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
            host=os.getenv("PGHOST"),
            port=int(os.getenv("PGPORT")),
            user=os.getenv("PGUSER"),
            password=os.getenv("PGPASSWORD"),
            database=os.getenv("PGDATABASE"),
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
            topic_title = f"Чат {anon_id[:4]}"
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


@dp.message(Command("start"))
async def start_command(message: types.Message):
    try:
        anon_id, topic_id = await register_user(message.from_user.id)

        # Получаем номер строки в базе
        async with db_pool2.acquire() as conn:
            result = await conn.fetchrow(
                "SELECT id FROM an_users WHERE telegram_id = $1", message.from_user.id
            )

        if result:
            user_number = result["id"]
            topic_name = f"{user_number}"

            await message.answer(
                f"Добро пожаловать! Здесь вы можете написать свой запрос. Вы общаетесь с администраторами через бота, поэтому вы остаетесь для них анонимными."
            )
    except Exception as e:
        logging.error(f"Ошибка при создании топика: {e}")
        await message.answer("Произошла ошибка при создании вашей темы.")


# Обработчик сообщений от пользователя
@dp.message(F.chat.type == "private")
async def handle_user_message(message: types.Message):
    logging.info(f"Сообщение от {message.from_user.id}: {message.text}")

    # Регистрируем пользователя и получаем данные
    anon_id, topic_id = await register_user(message.from_user.id)

    # Формируем сообщение для отправки в топик
    forward_message = f"Сообщение от {str(anon_id)[:4]}:\n{message.text}"

    # Отправляем сообщение в топик
    await bot.send_message(
        chat_id=GROUP_ID, message_thread_id=topic_id, text=forward_message
    )

    # Уведомляем пользователя
    await message.answer("Ваше сообщение отправлено администратору.")


@dp.message(F.chat.type.in_(["group", "supergroup"]) & ~F.text.startswith("/"))
async def handle_admin_reply(message: types.Message):
    """
    Обрабатывает новые сообщения от администратора в топиках группы.
    """
    await process_admin_message(message)


# Обработка новых сообщений администратора
@dp.message(F.chat.type.in_(["group", "supergroup"]) & ~F.text.startswith("/"))
async def handle_admin_reply(message: types.Message):
    """
    Обрабатывает только текстовые сообщения от администратора в топиках группы,
    игнорируя команды.
    """
    # Если сообщение пустое или команда, игнорируем его
    if not message.text or message.text.startswith("/"):
        logging.info(f"Игнорирование команды или пустого сообщения: {message.text}")
        return

    # Если сообщение подходит под условия, обрабатываем его
    await process_admin_message(message)


# Обработка редактирования сообщений администратора
@dp.edited_message(F.chat.type.in_(["group", "supergroup"]))
async def handle_admin_edited_message(message: types.Message):
    """
    Обрабатывает редактированные сообщения от администратора в топиках группы.
    """
    await process_admin_message(message)


# Общая функция обработки сообщений
async def process_admin_message(message: types.Message):
    """
    Обрабатывает как новые, так и редактированные сообщения от администратора.
    """
    logging.info(
        f"Обработка сообщения от администратора: {message.text}, чат: {message.chat.id}"
    )

    try:
        # Получаем topic_id из текущего чата
        topic_id = message.message_thread_id

        # Находим telegram_id пользователя, связанного с этим topic_id
        async with db_pool2.acquire() as conn:
            result = await conn.fetchrow(
                "SELECT telegram_id FROM an_users WHERE topic_id = $1", topic_id
            )

        if result:
            telegram_id = result["telegram_id"]

            # Пересылаем сообщение пользователю
            await bot.send_message(
                chat_id=telegram_id, text=f"Ответ от администратора:\n\n{message.text}"
            )
            logging.info(f"Ответ успешно отправлен пользователю с ID {telegram_id}")
        else:
            await message.reply(
                "Ошибка: Не удалось найти пользователя для данного топика."
            )
            logging.error(f"Не найден telegram_id для topic_id: {topic_id}")

    except Exception as e:
        logging.error(f"Ошибка при обработке ответа администратора: {e}")
        await message.reply("Произошла ошибка при отправке ответа пользователю.")


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
