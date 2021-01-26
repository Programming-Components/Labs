import sqlite3
import pika
import time

receiver_one = sqlite3.connect('receiver_one.db')
rabbit_conn = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
receiver_one.execute("delete from mes_results; ")
receiver_one.commit()
channel = rabbit_conn.channel()

channel.queue_declare(queue='team6_queue', durable=True)


def get_values(message):
    split_mes = message.split()
    index = int(split_mes[0][:-1])
    text = split_mes[1] + ' ' + split_mes[2]
    mes_time = int(split_mes[3])
    return index, text, mes_time


def callback(ch, method, properties, body):
    print("Receiver_one got the message!")
    print(body.decode('utf-8'))
    index, text, mes_time = get_values(body.decode('utf-8'))

    time.sleep(int(mes_time))
    receiver_one.execute(f"INSERT INTO MES_RESULTS (TEXT, TIME, I) VALUES ('{text}', {mes_time}, {index});")
    receiver_one.commit()
    ch.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue='team6_queue', on_message_callback=callback)

channel.start_consuming()
