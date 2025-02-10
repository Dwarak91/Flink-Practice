import random
import time
import calendar
from random import randint, choice
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from json import dumps
from time import sleep

# Kafka Configuration
KAFKA_BROKER = "kafka:9092"
PAYMENT_TOPIC = "payment_msg"
TRANSACTIONS_TOPIC = "transactions"
SENSOR_TOPIC = "sensor.readings"

# Transaction Types
TRANSACTION_TYPES = ["purchase", "refund", "transfer", "deposit"]


def generate_payment():
    """
    Generates a random payment event.
    """
    order_id = calendar.timegm(time.gmtime())
    ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    pay_amount = round(random.uniform(10, 100000), 2)  # Random payment amount
    pay_platform = 0 if random.random() < 0.9 else 1  # 90% probability for platform 0
    province_id = randint(0, 6)

    return {
        "createTime": ts,
        "orderId": order_id,
        "payAmount": pay_amount,
        "payPlatform": pay_platform,
        "provinceId": province_id
    }


def generate_transaction():
    """
    Generates a random transaction event.
    """
    transaction_id = f"txn-{randint(1000, 9999)}"
    amount = round(random.uniform(10, 5000), 2)  # Random amount between 10 and 5000
    timestamp = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime())  # ISO 8601 timestamp
    transaction_type = choice(TRANSACTION_TYPES)  # Random transaction type

    return {
        "transaction_id": transaction_id,
        "amount": amount,
        "timestamp": timestamp,
        "transaction_type": transaction_type
    }


def generate_sensor_event():
    """
    Generates a random sensor event.
    """
    DEVICES = ['b8:27:eb:bf:9d:51', '00:0f:00:70:91:0a', '1c:bf:ce:15:ec:4d']
    device_id = random.choice(DEVICES)
    co = round(random.uniform(0.0011, 0.0072), 4)
    humidity = round(random.uniform(45.00, 78.00), 2)
    motion = random.choice([True, False])
    temp = round(random.uniform(17.00, 36.00), 2)
    amp_hr = round(random.uniform(0.10, 1.80), 2)
    event_ts = int(time.time() * 1000)

    return {
        "device_id": device_id,
        "co": co,
        "humidity": humidity,
        "motion": motion,
        "temp": temp,
        "ampere_hour": amp_hr,
        "ts": event_ts
    }


def publish_messages(producer, num_messages=1000, delay=0.5):
    """
    Publishes messages to 'payment_msg', 'transactions', and 'sensor.readings' topics.
    
    :param producer: Kafka producer instance.
    :param num_messages: Number of messages to send.
    :param delay: Time delay (seconds) between messages.
    """
    for _ in range(num_messages):
        # Publish Payment Message
        payment = generate_payment()
        producer.send(PAYMENT_TOPIC, value=payment)
        print(f"Published to {PAYMENT_TOPIC}: {payment}")

        # Publish Transaction Message
        transaction = generate_transaction()
        producer.send(TRANSACTIONS_TOPIC, value=transaction)
        print(f"Published to {TRANSACTIONS_TOPIC}: {transaction}")

        # Publish Sensor Event Message
        sensor_event = generate_sensor_event()
        producer.send(SENSOR_TOPIC, value=sensor_event)
        print(f"Published to {SENSOR_TOPIC}: {sensor_event}")

        sleep(delay)  # Simulating real-time publishing


def create_producer():
    """
    Creates and returns a Kafka producer with JSON serialization.
    """
    print("Connecting to Kafka brokers...")
    for _ in range(6):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda x: dumps(x).encode("utf-8")
            )
            print("Connected to Kafka successfully.")
            return producer
        except NoBrokersAvailable:
            print("Waiting for Kafka brokers to be available...")
            sleep(10)
    
    raise RuntimeError("Failed to connect to Kafka brokers after multiple attempts.")


if __name__ == "__main__":
    producer = create_producer()
    publish_messages(producer, num_messages=1000, delay=0.5)
