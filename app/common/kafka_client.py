import json
import os

from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv("configs/.env")


class KafkaClient:

    @staticmethod
    def get_producer():

        producer = KafkaProducer(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
            value_serializer=lambda value: json.dumps(value).encode("utf-8")
        )

        return producer