#!/usr/bin/env python
from time import sleep

import pika

requests = []
EXCHANGE = 'logs'
Q_REQUESTS = 'requests'
Q_REPLY = 'amq.rabbitmq.reply-to'


def enter_critical_section(seconds):
    print('ENTER critical section')
    for i in range(0,5*seconds):
        sleep(0.2)
        print('.')
    print('EXIT critical section')

def callback(ch, method, properties, body):
    print(" [x] Received %r" % (body,))

def send_broadast(msg):
    channel.basic_publish(exchange=EXCHANGE, routing_key='', body=msg)

if __name__ == '__main__':
    # start connection
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    # connect to exchange broadcast
    channel.exchange_declare(exchange=EXCHANGE, exchange_type='fanout')

    # create own queue
    # exclusive tag is for deleting the queue when disconnecting
    result = channel.queue_declare(exclusive=True)
    own_queue_name = result.method.queue
    # bind own queue to exchange
    channel.queue_bind(exchange=EXCHANGE, queue=own_queue_name)

    # send a message
    send_broadast('this msg was broadcasted')

    channel.basic_consume(callback, queue=own_queue_name, no_ack=True)
    channel.start_consuming()

