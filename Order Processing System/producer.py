# This services publishes orders to the rabbitmq

import pika
import json

# connection to rabbitmq server
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()


# declare a queue(ensure queue is exists)
channel.queue_declare(queue='order_queue')

def place_order(order_id, product, quantity):
    """function to place an order"""
    order = {
        'order_id':order_id,
        'product':product,
        'quantity':quantity
    }

    # publish order to rabbitmq server

    channel.basic_publish(
        exchange='',
        routing_key='order_queue',
        body=json.dumps(order)
    )

    print(f"📦 Order placed: {order}")


# Place some orders
place_order(101, 'Laptop', 1)
place_order(102, 'Mobile', 2)
place_order(103, 'TV', 3)

# Close connection
connection.close()
