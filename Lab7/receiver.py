import sqlite3
import pika
import time

receiver_one = sqlite3.connect('receiver_one.db')
rabbit_conn = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
channel = rabbit_conn.channel()

channel.queue_declare(queue='team6_queue', durable=True)

def callback(ch, method, properties, body):
    print("Receieved message!")
    text = body.split()[2][:-1]
    print(text)
    complexity = body.split()[-1]
    time.sleep(int(complexity))
    ch.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue='team6_queue', on_message_callback=callback)

channel.start_consuming()