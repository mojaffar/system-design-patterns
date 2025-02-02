import pika
import json
import time

# Connect to RabbitMQ server
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Declare queue (ensures queue exists)
channel.queue_declare(queue='order_queue')


def process_order(ch, method, properties, body):
    """Function to process the order"""
    order = json.loads(body)

    print(f"✅ Processing Order: {order['order_id']} - {order['product']} x{order['quantity']}")

    # Simulate processing delay
    time.sleep(3)

    print(f"🎉 Order {order['order_id']} completed!\n")

    # Acknowledge message after processing
    ch.basic_ack(delivery_tag=method.delivery_tag)


# Listen to queue and consume messages
channel.basic_consume(queue='order_queue', on_message_callback=process_order)

print("🛒 Waiting for orders... Press CTRL+C to exit.")
channel.start_consuming()
