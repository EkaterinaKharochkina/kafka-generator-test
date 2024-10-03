from confluent_kafka.admin import AdminClient, NewTopic
import os
from dotenv import load_dotenv
load_dotenv()

admin_client = AdminClient({'bootstrap.servers': os.getenv('IP')})
new_topics = [
    NewTopic("test1", num_partitions=1, replication_factor=1)
]
fs = admin_client.create_topics(new_topics)
for topic, f in fs.items():
    try:
        f.result()
        print(f"Топик '{topic}' успешно создан.")
    except Exception as e:
        print(f"Ошибка при создании топика '{topic}': {e}")