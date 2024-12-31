import asyncio
from asyncio import Lock
import asyncpg
from asyncpg import create_pool
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
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


DATABASE_URL = os.getenv("DATABASE_URL")


# Асинхронная функция для создания пула подключения
async def get_db_pool2():
    try:
        if not DATABASE_URL:
            raise ValueError("DATABASE_URL не задана.")

        return await asyncpg.create_pool(DATABASE_URL, max_size=10)
    except Exception as e:
        logging.error(f"Ошибка при подключении к базе данных: {e}")
        raise


BOT_TOKEN = os.getenv("BOT_TOKEN")
GROUP_ID = os.getenv("GROUP_ID")  # Telegram ID администратора

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

    anon_id, topic_id = await register_user(message.from_user.id)

    # Создаем кнопки
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Написать волонтер_ке")],
            [KeyboardButton(text="Заполнить форму")],
        ],
        resize_keyboard=True,
    )

    await message.answer(
        "Привет 👋! Выбери ниже то, что тебе нужно. ✨\n Если мы не ответили тебе втечение 24 ч, то напиши нам на почту: rloveplus@proton.me или в Matrix Element: @br:bark.lgbt \n А так же желаем хорошего тебе дня 💖",
        reply_markup=keyboard,
    )


# Хэндлер для кнопки "Заполнить форму"
@dp.message(F.text == "Заполнить форму")
async def fill_form(message: types.Message):
    form_url = "https://t.me/InstantFormsBot/form?startapp=9dd0908d-3c39-45cb-afe1-7f4004aa8fc6&startApp=9dd0908d-3c39-45cb-afe1-7f4004aa8fc6"  # Ссылка на вашу форму
    await message.answer(f"[Заполнить форму]({form_url})", parse_mode="Markdown")


@dp.message(F.text == "Написать волонтер_ке")
async def contact_volunteer(message: types.Message):
    try:

        # Получаем номер строки в базе
        async with db_pool2.acquire() as conn:
            result = await conn.fetchrow(
                "SELECT id FROM an_users WHERE telegram_id = $1", message.from_user.id
            )

        if result:
            user_number = result["id"]
            topic_name = f"{user_number}"

            await message.answer(
                f"Добро пожаловать! Здесь ты можешь написать нам. Ты будешь общаться с администраторами через бота, поэтому ты останешься для них анонимными.",
                reply_markup=ReplyKeyboardRemove(),
            )
    except Exception as e:
        logging.error(f"Ошибка при создании топика: {e}")


# Обработчик сообщений от пользователя
@dp.message(F.chat.type == "private")
async def handle_user_message(message: types.Message):
    logging.info(
        f"Сообщение от {message.from_user.id}: {message.text or 'мультимедиа'}"
    )

    # Регистрируем пользователя и получаем данные
    anon_id, topic_id = await register_user(message.from_user.id)

    try:
        # Формируем идентификатор для анонимности
        user_tag = f"Сообщение от {str(anon_id)[:4]}:"

        # Отправляем разные типы сообщений
        if message.text:
            forward_message = f"{user_tag}\n{message.text}"
            await bot.send_message(
                chat_id=GROUP_ID, message_thread_id=topic_id, text=forward_message
            )
        elif message.photo:
            await bot.send_photo(
                chat_id=GROUP_ID,
                message_thread_id=topic_id,
                photo=message.photo[-1].file_id,
                caption=f"{user_tag}\n{message.caption or ''}",
            )
        elif message.video:
            await bot.send_video(
                chat_id=GROUP_ID,
                message_thread_id=topic_id,
                video=message.video.file_id,
                caption=f"{user_tag}\n{message.caption or ''}",
            )
        elif message.document:
            await bot.send_document(
                chat_id=GROUP_ID,
                message_thread_id=topic_id,
                document=message.document.file_id,
                caption=f"{user_tag}\n{message.caption or ''}",
            )
        elif message.audio:
            await bot.send_audio(
                chat_id=GROUP_ID,
                message_thread_id=topic_id,
                audio=message.audio.file_id,
                caption=f"{user_tag}\n{message.caption or ''}",
            )
        elif message.voice:
            await bot.send_voice(
                chat_id=GROUP_ID,
                message_thread_id=topic_id,
                voice=message.voice.file_id,
                caption=user_tag,
            )
        else:
            await bot.send_message(
                chat_id=GROUP_ID,
                message_thread_id=topic_id,
                text=f"{user_tag}\nТип сообщения пока не поддерживается.",
            )

        # Уведомляем пользователя
        await message.answer("Сообщение отправлено!")
        logging.info(f"Сообщение от {message.from_user.id} успешно переслано в топик.")

    except Exception as e:
        logging.error(f"Ошибка при обработке сообщения от пользователя: {e}")
        await message.answer("Произошла ошибка при отправке сообщения.")


# Обработка новых сообщений администратора
@dp.message(F.chat.type.in_(["group", "supergroup"]) & ~F.text.startswith("/"))
async def handle_admin_reply(message: types.Message):
    """
    Обрабатывает только текстовые сообщения от администратора в топиках группы,
    игнорируя команды.
    """
    if not any(
        [
            message.text,
            message.photo,
            message.video,
            message.document,
            message.audio,
            message.voice,
        ]
    ):
        logging.info(f"Игнорирование пустого сообщения или команды: {message.text}")
        return

        # Обрабатываем сообщение
    await process_admin_message(message)


# Обработка редактирования сообщений администратора
@dp.edited_message(F.chat.type.in_(["group", "supergroup"]))
async def handle_admin_edited_message(message: types.Message):
    """
    Обрабатывает редактированные сообщения от администратора в топиках группы.
    """
    topic_id = message.message_thread_id

    # Проверяем, существует ли topic_id в базе
    async with db_pool2.acquire() as conn:
        result = await conn.fetchrow(
            "SELECT telegram_id FROM an_users WHERE topic_id = $1", topic_id
        )

    if not result:
        logging.warning(
            f"Редактирование сообщения в несуществующем топике: {topic_id}. Игнорируем."
        )
        return  # Игнорируем редактирование, если пользователь не зарегистрирован

    # Если пользователь найден, продолжаем обработку
    await process_admin_message(message)


# Общая функция обработки сообщений
async def process_admin_message(message: types.Message):
    """
    Обрабатывает как новые, так и редактированные сообщения от администратора.
    """
    logging.info(
        f"Обработка сообщения от администратора: {message.text}, чат: {message.chat.id}"
    )
    # Получаем topic_id из текущего чата
    topic_id = message.message_thread_id

    # Находим telegram_id пользователя, связанного с этим topic_id
    async with db_pool2.acquire() as conn:
        result = await conn.fetchrow(
            "SELECT telegram_id FROM an_users WHERE topic_id = $1", topic_id
        )

    try:
        telegram_id = result["telegram_id"]

        # Пересылаем сообщение пользователю в зависимости от типа контента
        if message.photo:
            await bot.send_photo(
                chat_id=telegram_id,
                photo=message.photo[-1].file_id,
                caption=message.caption,
            )
        elif message.video:
            await bot.send_video(
                chat_id=telegram_id,
                video=message.video.file_id,
                caption=message.caption,
            )
        elif message.document:
            await bot.send_document(
                chat_id=telegram_id,
                document=message.document.file_id,
                caption=message.caption,
            )
        elif message.audio:
            await bot.send_audio(
                chat_id=telegram_id,
                audio=message.audio.file_id,
                caption=message.caption,
            )
        elif message.voice:
            await bot.send_voice(
                chat_id=telegram_id,
                voice=message.voice.file_id,
                caption=message.caption,
            )
        elif message.text:
            await bot.send_message(
                chat_id=telegram_id, text=f"Ответ волонтёр_ки:\n\n{message.text}"
            )

        logging.info(f"Ответ успешно отправлен пользователю с ID {telegram_id}")

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
