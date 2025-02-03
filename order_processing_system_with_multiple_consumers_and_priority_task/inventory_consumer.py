import pika
import json
import time

# Connect to RabbitMQ server
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Declare inventory queue
channel.queue_declare(queue='inventory_queue')


def process_inventory(ch, method, properties, body):
    """Function to process inventory"""
    order = json.loads(body)

    print(f"🔧 Updating Inventory for Order: {order['order_id']} - {order['product']} x{order['quantity']}")
    time.sleep(2)  # Simulate inventory update time

    print(f"✔️ Inventory updated for Order {order['order_id']}!\n")

    # Acknowledge message after processing
    ch.basic_ack(delivery_tag=method.delivery_tag)


# Listen to inventory queue
channel.basic_consume(queue='inventory_queue', on_message_callback=process_inventory)

print("📦 Waiting for inventory updates... Press CTRL+C to exit.")
channel.start_consuming()
