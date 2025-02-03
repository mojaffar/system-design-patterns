import pika
import json
import time

# Connect to RabbitMQ server
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Declare notification queue
channel.queue_declare(queue='notification_queue')


def send_notification(ch, method, properties, body):
    """Function to send notifications"""
    order = json.loads(body)

    print(f"📧 Sending Notification for Order: {order['order_id']} - {order['product']} x{order['quantity']}")
    time.sleep(1)  # Simulate notification sending time

    print(f"✔️ Notification sent for Order {order['order_id']}!\n")

    # Acknowledge message after processing
    ch.basic_ack(delivery_tag=method.delivery_tag)


# Listen to notification queue
channel.basic_consume(queue='notification_queue', on_message_callback=send_notification)

print("📬 Waiting for notification orders... Press CTRL+C to exit.")
channel.start_consuming()
