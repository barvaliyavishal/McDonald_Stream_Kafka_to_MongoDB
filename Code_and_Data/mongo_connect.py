import json
import os
import signal
import sys
from typing import Dict, Optional

from confluent_kafka import KafkaError
from confluent_kafka.deserializing_consumer import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.collection import Collection

load_dotenv()

RUNNING = True


def handle_signal(signum, frame):
    del signum, frame
    global RUNNING
    RUNNING = False


def get_required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def build_kafka_auth_config() -> Dict[str, str]:
    return {
        "bootstrap.servers": get_required_env("KAFKA_BOOTSTRAP_SERVERS"),
        "security.protocol": os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL"),
        "sasl.mechanisms": os.getenv("KAFKA_SASL_MECHANISMS", "PLAIN"),
        "sasl.username": get_required_env("KAFKA_API_KEY"),
        "sasl.password": get_required_env("KAFKA_API_SECRET"),
    }


def build_schema_registry_client() -> SchemaRegistryClient:
    sr_url = get_required_env("SCHEMA_REGISTRY_URL")
    sr_key = get_required_env("SCHEMA_REGISTRY_API_KEY")
    sr_secret = get_required_env("SCHEMA_REGISTRY_API_SECRET")
    return SchemaRegistryClient(
        {
            "url": sr_url,
            "basic.auth.user.info": f"{sr_key}:{sr_secret}",
        }
    )


def build_avro_deserializer(schema_registry_client: SchemaRegistryClient, subject: str) -> AvroDeserializer:
    schema_str = schema_registry_client.get_latest_version(subject).schema.schema_str
    return AvroDeserializer(schema_registry_client, schema_str)


def build_avro_deserializer_from_file(schema_registry_client: SchemaRegistryClient, schema_file: str) -> AvroDeserializer:
    with open(schema_file, "r", encoding="utf-8") as file:
        schema_str = file.read()
    return AvroDeserializer(schema_registry_client, schema_str)


def build_consumer(topic: str, group_id: str, avro_deserializer: AvroDeserializer) -> DeserializingConsumer:
    config = {
        **build_kafka_auth_config(),
        "group.id": group_id,
        "auto.offset.reset": os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest"),
        "enable.auto.commit": False,
        "key.deserializer": StringDeserializer("utf_8"),
        "value.deserializer": avro_deserializer,
    }
    consumer = DeserializingConsumer(config)
    consumer.subscribe([topic])
    return consumer


def upsert_order(collection: Collection, record: Dict):
    collection.update_one(
        {"order_id": record["order_id"]},
        {"$set": record},
        upsert=True,
    )


def upsert_payment(collection: Collection, record: Dict):
    collection.update_one(
        {"payment_id": record["payment_id"]},
        {"$set": record},
        upsert=True,
    )


def upsert_joined(collection: Collection, record: Dict):
    order_id = record.get("order_id")
    payment_id = record.get("payment_id")
    if not order_id or not payment_id:
        raise ValueError("Joined record must include order_id and payment_id")

    collection.update_one(
        {"order_id": order_id, "payment_id": payment_id},
        {"$set": record},
        upsert=True,
    )


def persist_record(collection: Collection, key: Optional[str], value: Optional[Dict], mode: str):
    if value is None:
        return

    if key:
        value["kafka_key"] = key

    if mode == "orders":
        upsert_order(collection, value)
    elif mode == "payments":
        upsert_payment(collection, value)
    else:
        upsert_joined(collection, value)


def main():
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    topic = get_required_env("CONSUME_TOPIC")
    group_id = os.getenv("KAFKA_CONSUMER_GROUP", "mongo-sink-group")
    mongo_uri = get_required_env("MONGODB_URI")
    mongo_db = os.getenv("MONGODB_DB", "mcdonalds")
    mongo_collection = os.getenv("MONGODB_COLLECTION", "orders_payments")
    mode = os.getenv("MONGO_SINK_MODE", "joined").lower()

    if mode not in {"orders", "payments", "joined"}:
        raise ValueError("MONGO_SINK_MODE must be one of: orders, payments, joined")

    topic_subject = os.getenv("CONSUME_SUBJECT", f"{topic}-value")
    orders_schema_file = os.getenv("ORDERS_SCHEMA_FILE", "orders_avro_schema.json")
    payments_schema_file = os.getenv("PAYMENTS_SCHEMA_FILE", "payments_avro_schema.json")

    schema_registry_client = build_schema_registry_client()
    try:
        avro_deserializer = build_avro_deserializer(schema_registry_client, topic_subject)
    except Exception:
        if mode == "orders":
            avro_deserializer = build_avro_deserializer_from_file(
                schema_registry_client, orders_schema_file
            )
        elif mode == "payments":
            avro_deserializer = build_avro_deserializer_from_file(
                schema_registry_client, payments_schema_file
            )
        else:
            raise

    consumer = build_consumer(topic, group_id, avro_deserializer)

    mongo_client = MongoClient(mongo_uri)
    collection = mongo_client[mongo_db][mongo_collection]

    print(
        json.dumps(
            {
                "status": "running",
                "topic": topic,
                "group_id": group_id,
                "mongo_db": mongo_db,
                "mongo_collection": mongo_collection,
                "mode": mode,
            }
        )
    )

    try:
        while RUNNING:
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(f"Kafka consumer error: {msg.error()}")

            persist_record(collection, msg.key(), msg.value(), mode)
            consumer.commit(msg)
    finally:
        consumer.close()
        mongo_client.close()
        print('{"status":"stopped"}')


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Fatal error: {exc}", file=sys.stderr)
        sys.exit(1)
