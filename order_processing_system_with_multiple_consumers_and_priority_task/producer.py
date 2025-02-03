import pika
import json

# Connect to RabbitMQ server
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Declare queues for different tasks (with priorities)
channel.queue_declare(queue='payment_queue', arguments={'x-max-priority': 10})  # Payment queue with high priority
channel.queue_declare(queue='inventory_queue')
channel.queue_declare(queue='notification_queue')


def place_order(order_id, product, quantity):
    """Function to place an order"""
    order = {
        'order_id': order_id,
        'product': product,
        'quantity': quantity
    }

    # Send message to the 'payment_queue' first (high priority)
    channel.basic_publish(
        exchange='',
        routing_key='payment_queue',  # Payment task routing key
        body=json.dumps(order),
        properties=pika.BasicProperties(priority=10)  # Highest priority
    )

    # Send message to 'inventory_queue' (medium priority)
    channel.basic_publish(
        exchange='',
        routing_key='inventory_queue',
        body=json.dumps(order),
        properties=pika.BasicProperties(priority=5)  # Medium priority
    )

    # Send message to 'notification_queue' (low priority)
    channel.basic_publish(
        exchange='',
        routing_key='notification_queue',
        body=json.dumps(order),
        properties=pika.BasicProperties(priority=1)  # Low priority
    )

    print(f"📦 Order placed: {order}")


# Place some orders
place_order(101, 'Laptop', 1)
place_order(102, 'Mobile', 2)

# Close connection
connection.close()
