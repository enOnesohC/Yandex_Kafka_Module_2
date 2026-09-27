import faust
import redis.asyncio as redis
import re
import os


def _build_broker() -> str:
    raw = os.getenv("KAFKA_BROKER", "localhost:9094")
    if "://" not in raw:
        raw = "kafka://" + raw
    return raw


BROKER = _build_broker()
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")


class Message(faust.Record):
    message_id: str
    sender_id: str
    recipient_id: str
    text: str
    timestamp: str


class BlockEvent(faust.Record, serializer='json'):
    sender_id: str
    recipient_id: str


app = faust.App(
    "processing_messages",
    broker=BROKER,
    store="memory://",
)

# Персистентная таблица блокировок.
# Ключ: "sender_id:recipient_id".
blocked_users = app.Table(
    'blocked_users',
    default=bool,
    partitions=3,
)

messages_topic = app.topic("messages", key_type=str, value_type=Message)
filtered_messages_topic = app.topic("filtered_messages", key_type=str, value_type=Message)
blocks_topic = app.topic('blocked_users', value_type=BlockEvent)


@app.agent(blocks_topic)
async def process_blocks(events):
    async for event in events:
        key = f'{event.recipient_id}:{event.sender_id}'
        blocked_users[key] = True


@app.agent(messages_topic)
async def calculate_messages(stream):
    """
    Обработка сообщений в топике messages
    Сначала ищем ключ в таблице заблокированных пользователей
    Если находим, то сообщение не отправляем
    Перед отправлением проверяем слова на badwords из redis
    И выполняем замену на ***
    :param stream: Поток сообщений в топике messages
    :return: None
    """
    async for message in stream:
        key = f'{message.recipient_id}:{message.sender_id}'
        if blocked_users.get(key, False):
            print(f"В таблице blocked_users обнаружен ключ {key}, отправка сообщения заблокирована")
            continue

        r = redis.from_url(REDIS_URL, decode_responses=True)
        badwords = set(await r.lrange("badwords", 0, -1))

        filtered_message = replace_badwords(message.text, badwords)
        await filtered_messages_topic.send(value=Message(
            message_id=message.message_id,
            sender_id=message.sender_id,
            recipient_id=message.recipient_id,
            text=filtered_message,
            timestamp=message.timestamp))


def replace_badwords(text: str, badwords: set[str]) -> str:
    """
    Выполняет замену слов в тексте сообщения на ***
    from deepseek
    :param text:
    :param badwords:
    :return:
    """
    if not badwords:
        return text
    pattern = re.compile(
        "|".join(re.escape(w) for w in sorted(badwords, key=len, reverse=True)),
        re.IGNORECASE,
    )
    return pattern.sub("***", text)
