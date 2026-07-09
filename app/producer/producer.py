import os
import random
import time
from datetime import datetime

from dotenv import load_dotenv

from app.common.kafka_client import KafkaClient

load_dotenv("configs/.env")

producer = KafkaClient.get_producer()

topic = os.getenv("KAFKA_TOPIC")

symbol = os.getenv("STOCK_SYMBOL")

price_min = float(os.getenv("PRICE_MIN"))
price_max = float(os.getenv("PRICE_MAX"))

print("=" * 60)
print(" STOCK PRICE PRODUCER STARTED ")
print("=" * 60)

while True:

    event = {

        "symbol": symbol,

        "price": round(
            random.uniform(price_min, price_max),
            2
        ),

        "event_time": datetime.utcnow().isoformat(timespec='milliseconds')

    }

    producer.send(topic, event)

    producer.flush()

    print(event)

    time.sleep(1)