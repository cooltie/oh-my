# telegram topic's bot:
It helps to communicate with subscribers anonymously:
User will write to bot.
You and other admins (if needed) will see messages in your channel.
Each user = topic in your channel.
!!! Do not create and delete new topics manually.



Before hand you need to:
1. create channel with topics
2. create bot in telegram
3. male bot admin in channel allowing to create new topics
4. create database
5. create .env file and set 4 variables:
- BOT_TOKEN = bot token from telegram
- GROUP_ID = id of your channel
- DATABASE_URL = address of your database
- TABLE_NAME = name of your table in database (
     CREATE TABLE table name here (
                id SERIAL PRIMARY KEY,
                telegram_id BIGINT NOT NULL,
                anon_id UUID NOT NULL,
                created_at TIMESTAMP DEFAULT NOW()
		        topic_id INTEGER NOT NULL
)
6. connect database to bot by setting needed variables
6. run bot
7. enjoy
