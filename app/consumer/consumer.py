import os

from dotenv import load_dotenv
from kafka import KafkaConsumer
import json

load_dotenv("configs/.env")

consumer = KafkaConsumer(

    os.getenv("KAFKA_TOPIC"),

    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),

    auto_offset_reset="earliest",

    value_deserializer=lambda m: json.loads(m.decode("utf-8"))

)

print("=" * 60)
print("CONSUMER STARTED")
print("=" * 60)

for message in consumer:

    print(message.value)