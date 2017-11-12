#!/usr/bin/env python
from time import sleep
import pika
import threading

from core import *

try:
    from pprint import pprint
except:
    pprint = print

EXCHANGE = 'logs'
Q_REQUESTS = 'requests'
MSG_REQUEST = 'REQUEST'

global requests
global own_queue_name


def enter_critical_section(seconds):
    print('ENTER critical section ')
    for _ in range(0, 5*seconds):
        sleep(0.2)
        print('.')
    print('EXIT critical section')


def send_broadcast(msg):
    channel.basic_publish(exchange=EXCHANGE, routing_key='', body=msg)


def send_request(request_time):
    request = Request(request_time, own_queue_name)
    send_broadcast(MSG_REQUEST)


def receive_data():
    global own_queue_name

    def callback(ch, method, properties, body):
        print(" [x] Received %r" % (body,))

    # start to listen to broadcasted msgs
    channel.basic_consume(callback, queue=own_queue_name, no_ack=True)
    channel.start_consuming()


# used by user interface thread
def read_keyboard():
    print("Type requests to send: \t")

    while 1:
        try:
            user_input = input("> ")
            request_time = int(user_input)
            send_request(request_time)
        except:
            print("Your message should be a integer")
            continue


def run_threads():
    # start a thread to receive messages
    receiver_thread = threading.Thread(target=receive_data)
    receiver_thread.daemon = True
    receiver_thread.start()

    thread_user = threading.Thread(target=read_keyboard)
    thread_user.daemon = True
    thread_user.start()

    # hang program execution
    while 1:
        sleep(10)


if __name__ == '__main__':
    global own_queue_name

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

    run_threads()
