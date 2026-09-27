# seed_ban_users.py
import json
from confluent_kafka import Producer


class SeedBanUsers:
    BOOTSTRAP = 'localhost:9094'
    TOPIC = 'blocked_users'

    BLOCKS = {
        'u1': ['u2'],        # u1 блокирует u2 -> в таблицу u2:u1
        'u2': ['u1'],        # u2 блокирует u1 -> u1:u2
        'u3': ['u4', 'u5'],  # u4:u3, u5:u3
        'u6': ['u7'],        # u7:u6
        'u10': ['u1'],       # u1:u10
    }

    def __init__(self, bootstrap: str = BOOTSTRAP, topic: str = TOPIC) -> None:
        self.bootstrap = bootstrap
        self.topic = topic
        self.producer = Producer({
            'bootstrap.servers': self.bootstrap,
            'client.id': 'seed-blocks',
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

    def fill_parameters(self) -> None:
        for owner, blocked_list in self.BLOCKS.items():
            for blocked in blocked_list:
                event = {'sender_id': blocked, 'recipient_id': owner}
                key = f'{blocked}:{owner}'
                self.producer.produce(
                    topic=self.topic,
                    key=key,
                    value=json.dumps(event).encode('utf-8'),
                    callback=self.delivery_report,  # без скобок
                )
                self.producer.poll(0)
                self.sent += 1

        self.producer.flush(timeout=10)
        print(f'sent {self.sent} block events')
