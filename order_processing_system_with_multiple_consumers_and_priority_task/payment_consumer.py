import pika
import json
import time

# Connect to RabbitMQ server
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Declare payment queue with priority support
channel.queue_declare(queue='payment_queue', arguments={'x-max-priority': 10})


def process_payment(ch, method, properties, body):
    """Function to process payments"""
    order = json.loads(body)

    print(f"✅ Processing Payment for Order: {order['order_id']} - {order['product']} x{order['quantity']}")
    time.sleep(3)  # Simulate payment processing time

    print(f"🎉 Payment completed for Order {order['order_id']}!\n")

    # Acknowledge message after processing
    ch.basic_ack(delivery_tag=method.delivery_tag)


# Listen to payment queue
channel.basic_consume(queue='payment_queue', on_message_callback=process_payment)

print("💳 Waiting for payment orders... Press CTRL+C to exit.")
channel.start_consuming()
