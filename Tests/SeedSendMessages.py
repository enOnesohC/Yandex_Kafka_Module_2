import json
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer


class SeedSendMessages:
    BOOTSTRAP = 'localhost:9094'
    TOPIC = 'messages'

    CASES = [
        ('u2', 'u1',  'hello badword'),
        ('u1', 'u2',  'hi there'),
        ('u4', 'u3',  'badword1 badword2 badword3'),
        ('u5', 'u3',  'badword1'),
        ('u7', 'u6',  'ok'),
        ('u1', 'u10', 'text badword'),
        ('u1', 'u3',  'hello badword, badword1, badword2'),
        ('u6', 'u5',  'clean text'),
        ('u8', 'u9',  'badword1 badword and normal'),
        ('u9', 'u8',  'just a message'),
    ]

    def __init__(self, bootstrap: str = BOOTSTRAP, topic: str = TOPIC) -> None:
        self.bootstrap = bootstrap
        self.topic = topic
        self.producer = Producer({
            'bootstrap.servers': self.bootstrap,
            'client.id': 'seed-messages',
            'acks': 'all',
            'enable.idempotence': True,
        })
        self.sent = 0

    @staticmethod
    def delivery_report(err, msg) -> None:
        if err is not None:
            print(f'FAILED {msg.key()}: {err}')
        else:
            print(f'OK {msg.key()} -> {msg.topic()}[{msg.partition()}]@{msg.offset()}')

    @staticmethod
    def _build_message(sender: str, recipient: str, text: str) -> dict:
        return {
            'message_id': str(uuid.uuid4()),
            'sender_id': sender,
            'recipient_id': recipient,
            'text': text,
            'timestamp': datetime.now(timezone.utc).isoformat(),
        }

    def fill_parameters(self) -> None:
        for sender, recipient, text in self.CASES:
            message = self._build_message(sender, recipient, text)
            self.producer.produce(
                topic=self.topic,
                key=f"{recipient}:{sender}",
                value=json.dumps(message).encode('utf-8'),
                callback=self.delivery_report,
            )
            self.producer.poll(0)
            self.sent += 1

        self.producer.flush(timeout=10)
        print(f'sent {self.sent} messages')
