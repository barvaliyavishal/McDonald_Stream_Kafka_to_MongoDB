import os
import random
import time
import uuid
from datetime import datetime

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
from dotenv import load_dotenv

load_dotenv()

def delivery_report(err, msg):
    """
    Reports the failure or success of a message delivery.

    Args:
        err (KafkaError): The error that occurred on None on success.

        msg (Message): The message that was produced or failed.
    """
    if err is not None:
        print(f"Delivery failed for User record {msg.key()}: {err}")
        return
    print(f"User record {msg.key()} successfully produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
    print("====================================")


def get_required_env(name):
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def build_kafka_config():
    return {
        "bootstrap.servers": get_required_env("KAFKA_BOOTSTRAP_SERVERS"),
        "sasl.mechanisms": os.getenv("KAFKA_SASL_MECHANISMS", "PLAIN"),
        "security.protocol": os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL"),
        "sasl.username": get_required_env("KAFKA_API_KEY"),
        "sasl.password": get_required_env("KAFKA_API_SECRET"),
    }


def build_schema_registry_client():
    sr_url = get_required_env("SCHEMA_REGISTRY_URL")
    sr_key = get_required_env("SCHEMA_REGISTRY_API_KEY")
    sr_secret = get_required_env("SCHEMA_REGISTRY_API_SECRET")
    return SchemaRegistryClient(
        {
            "url": sr_url,
            "basic.auth.user.info": f"{sr_key}:{sr_secret}",
        }
    )


key_serializer = StringSerializer("utf_8")

# Fetch the latest schema dynamically
def get_latest_schema(schema_registry_client, subject):
    schema = schema_registry_client.get_latest_version(subject).schema.schema_str
    return AvroSerializer(schema_registry_client, schema)


def get_schema_from_file(schema_file):
    with open(schema_file, "r", encoding="utf-8") as file:
        return file.read()


def get_value_serializer(schema_registry_client, subject, schema_file):
    try:
        return get_latest_schema(schema_registry_client, subject)
    except Exception:
        local_schema = get_schema_from_file(schema_file)
        return AvroSerializer(schema_registry_client, local_schema)

# Valid McDonald's menu items
menu_items = [
    "Big Mac", "McChicken", "Quarter Pounder", "French Fries", "McFlurry",
    "Filet-O-Fish", "Chicken McNuggets", "Egg McMuffin", "Hash Browns", "Apple Pie"
]

# Mock data generation

def generate_orders_and_payments(orders_producer, payments_producer, message_count, sleep_seconds):
    utc_now = int(datetime.utcnow().timestamp() * 1000)

    for _ in range(message_count):
        # Generate matching order and payment data
        order_id = str(uuid.uuid4())
        customer_id = f"cust_{random.randint(1000, 9999)}"
        order_total = round(random.uniform(10, 100), 2)
        order_time = utc_now - random.randint(0, 24 * 60 * 60 * 1000)  # Random timestamp within 24 hours

        order_items = [
            {"item_name": random.choice(menu_items), "quantity": random.randint(1, 5), "price": round(random.uniform(1, 10), 2)}
            for _ in range(random.randint(1, 3))
        ]

        payment_id = str(uuid.uuid4())
        payment_amount = order_total
        payment_method = random.choice(["credit_card", "cash", "mobile_payment"])
        payment_time = order_time + random.randint(0, 5 * 60 * 1000)  # Random delay after order_time

        # Produce order
        orders_producer.produce(
            topic=ORDERS_TOPIC,
            key=order_id,
            value={
                "order_id": order_id,
                "customer_id": customer_id,
                "order_total": order_total,
                "order_items": order_items,
                "order_time": order_time
            },
            on_delivery=delivery_report
        )
        orders_producer.poll(0)

        # Produce payment
        payments_producer.produce(
            topic=PAYMENTS_TOPIC,
            key=payment_id,
            value={
                "payment_id": payment_id,
                "order_id": order_id,
                "payment_amount": payment_amount,
                "payment_method": payment_method,
                "payment_time": payment_time
            },
            on_delivery=delivery_report
        )
        payments_producer.poll(0)

        time.sleep(sleep_seconds)

    orders_producer.flush()
    payments_producer.flush()


def main():
    kafka_config = build_kafka_config()
    schema_registry_client = build_schema_registry_client()

    orders_topic = os.getenv("ORDERS_TOPIC", "macd_orders_raw")
    payments_topic = os.getenv("PAYMENTS_TOPIC", "macd_payments_raw")
    orders_subject = os.getenv("ORDERS_SUBJECT", f"{orders_topic}-value")
    payments_subject = os.getenv("PAYMENTS_SUBJECT", f"{payments_topic}-value")
    orders_schema_file = os.getenv("ORDERS_SCHEMA_FILE", "orders_avro_schema.json")
    payments_schema_file = os.getenv("PAYMENTS_SCHEMA_FILE", "payments_avro_schema.json")

    orders_producer = SerializingProducer(
        {
            **kafka_config,
            "key.serializer": key_serializer,
            "value.serializer": get_value_serializer(
                schema_registry_client, orders_subject, orders_schema_file
            ),
        }
    )
    payments_producer = SerializingProducer(
        {
            **kafka_config,
            "key.serializer": key_serializer,
            "value.serializer": get_value_serializer(
                schema_registry_client, payments_subject, payments_schema_file
            ),
        }
    )

    message_count = int(os.getenv("MOCK_MESSAGE_COUNT", "100"))
    sleep_seconds = float(os.getenv("MOCK_SLEEP_SECONDS", "1"))

    # Use locally scoped topic names so the producer works for custom environments too.
    global ORDERS_TOPIC, PAYMENTS_TOPIC
    ORDERS_TOPIC = orders_topic
    PAYMENTS_TOPIC = payments_topic

    generate_orders_and_payments(
        orders_producer=orders_producer,
        payments_producer=payments_producer,
        message_count=message_count,
        sleep_seconds=sleep_seconds,
    )
    print("Mock data successfully published.")


ORDERS_TOPIC = "macd_orders_raw"
PAYMENTS_TOPIC = "macd_payments_raw"


if __name__ == "__main__":
    main()