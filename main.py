import random
import json
import time
from datetime import datetime
from faker import Faker
from random import choice
from confluent_kafka import Producer
import os
from dotenv import load_dotenv
load_dotenv()
faker = Faker()

activity_types = ['deposit', 'withdraw', 'payment', 'identification']

producer_config = {'bootstrap.servers': os.getenv('IP')}
producer = Producer(producer_config)

def delivery_report(err, msg):
    if err is not None:
        print(f'Ошибка при доставке сообщения: {err}')
    else:
        print(f'Сообщение отправлено в топик {msg.topic()} с ключом {msg.key()}')

def gen_payment_activity(activity_count: int, topic_name: str, delay: float = 0) -> None:
    for _ in range(activity_count):
        payment_activity = {
            'card_number': faker.credit_card_number(),
            'payment_amount': random.randint(1, 10000),
            'activity_type': choice(activity_types),
            'currency_id': random.randint(1, 6),
            'current_time_payment': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        message = json.dumps(payment_activity)

        producer.produce(
            topic=topic_name,
            value=message,
            callback=delivery_report
        )
        producer.poll(0)
        time.sleep(delay)
    producer.flush()

gen_payment_activity(activity_count=10, topic_name='test1', delay=60)