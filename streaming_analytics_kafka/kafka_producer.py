from kafka import KafkaProducer
import json
import random
import time

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


def generate_transaction():
    """Generate a random transaction and send it to Kafka"""
    transaction = {
        'transaction_id': random.randint(1000, 9999),
        'amount': random.randint(10, 5000),
        'location': random.choice(['New York', 'London', 'Tokyo']),
        'status': 'pending'
    }

    producer.send('transactions', value=transaction)
    print(f"📤 Sent transaction: {transaction}")


# Simulate continuous transactions
while True:
    generate_transaction()
    time.sleep(1)
