#!/usr/bin/env python
from time import sleep
import pika
import threading

from core import *

try:
    from pprint import pprint
except:
    pprint = print

''' constants '''
EXCHANGE = 'broadcast'
MSG_REQUEST = 'REQUEST'
DEBUG_FLAG = '[DEBUG]'

requests = []


'''
Requesting process

    1- Pushing its request in its own queue (ordered by time stamps)
    2- Sending a request to every node.
    3- Waiting for replies from all other nodes.
    4- If own request is at the head of its queue and all replies have been received, enter critical section.
    5- Upon exiting the critical section, remove its request from the queue and send a release message to every process.
'''


def enter_critical_section(seconds):
    print('ENTER critical section ')
    for _ in range(0, 5*seconds):
        sleep(0.2)
        print('.')
    print('EXIT critical section')


def send_request(request_time):
    # 1- push request to own queue
    request = Request(request_time, own_queue_name)
    requests.append(request)

    # 2- broadcast request msg
    msg = MSG_REQUEST + ' ' + str(request_time)
    channel.basic_publish(exchange=EXCHANGE, routing_key=own_queue_name, body=msg)

    print(DEBUG_FLAG, 'request sent')


def receive_data():
    def callback(ch, method, properties, body):
        global waiting_responses

        # ignore own messages
        if method.routing_key == own_queue_name:
            print(DEBUG_FLAG, "ignoring own message")
            return

        print(DEBUG_FLAG, "received %r from %r" % (body, method.routing_key))

        # after receiving all responses, stop waiting
        # TODO: only set boolean as False after receiving ALL responses
        waiting_responses = False

    # start to listen to broadcast msgs
    channel.basic_consume(callback, queue=own_queue_name, no_ack=True)
    channel.start_consuming()


# used by user interface thread
def read_keyboard():
    global waiting_responses

    print("Type requests to send: \t")

    while 1:
        try:
            user_input = input("> ")
            # ignore messages when waiting responses
            if waiting_responses:
                print("Request ignored: waiting responses for last request")
                continue
            request_time = int(user_input)
            # send request and wait for responses
            send_request(request_time)
            waiting_responses = True
        except ValueError:
            print("Your request should be an integer")
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
    global waiting_responses

    waiting_responses = False

    # start connection
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    # connect to exchange broadcast
    channel.exchange_declare(exchange=EXCHANGE, exchange_type='fanout')

    # create own queue
    # exclusive tag is for deleting the queue when disconnecting
    result = channel.queue_declare(exclusive=True)
    own_queue_name = result.method.queue
    print("own queue name = %s", own_queue_name)

    # bind own queue to exchange
    channel.queue_bind(exchange=EXCHANGE, queue=own_queue_name)

    run_threads()
