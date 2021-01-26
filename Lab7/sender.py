import sqlite3
import pika
import time
import random

sender_db = sqlite3.connect('sender.db')
rabbit_conn = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = rabbit_conn.channel()

channel.queue_declare(queue='team6_queue', durable=True)

for i in range(30):
    time.sleep(2)
    rand_id = random.randrange(1,4)
    cursor = sender_db.execute(f'SELECT TEXT, TIME FROM MESSAGE m WHERE m.ID = {rand_id}')
    row = cursor.fetchone()
    message = f'{i}: {row[0]} {row[1]}'

    channel.basic_publish(
        exchange='',
        routing_key='team6_queue',
        body=message,
        properties=pika.BasicProperties(delivery_mode=2)
    )
    print(f'Sent : "{message}"')


rabbit_conn.close()
sender_db.close()