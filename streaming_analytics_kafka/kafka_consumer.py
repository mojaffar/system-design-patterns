from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

print("🛡️ Fraud Detection Service is running...")

for message in consumer:
    transaction = message.value
    print(f"🔍 Processing transaction: {transaction}")

    # Simple fraud detection: Flag transactions > $4000
    if transaction['amount'] > 4000:
        print(f"🚨 Fraud Alert! Transaction ID {transaction['transaction_id']} is suspicious!")

