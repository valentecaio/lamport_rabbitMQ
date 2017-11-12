#!/usr/bin/env python
from time import sleep
import pika
import threading
import queue

from core import *

try:
    from pprint import pprint
except:
    pprint = print


DEBUG = True

''' constants '''
EXCHANGE = 'broadcast'
DEBUG_FLAG = '[DEBUG]'
MSG_REQUEST = 'REQUEST'
MSG_RESPONSE = 'RESPONSE'
MSG_RELEASE = 'RELEASE'
MSG_RESPONSE_PERMISSION_GRANTED = 'GRANTED_PERMISSION'
MSG_RESPONSE_NO_PERMISSION_GRANTED = 'NO_PERMISSION'
MSG_SEPARATOR = ' '

# thread-safe requests queue
requests = queue.PriorityQueue()
clock = 0


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


def increment_clock():
    global clock
    clock += 1
    if DEBUG:
        print(DEBUG_FLAG, "incremented clock to", clock)


def send_request(access_duration):
    global requests

    # increment timestamp before creating request
    increment_clock()

    # 1- push request to own queue
    request = Request(clock, own_queue_name, access_duration)
    requests.put_nowait((request.timestamp, request))

    # 2- broadcast request msg
    msg = MSG_REQUEST + MSG_SEPARATOR + str(clock)
    channel.basic_publish(exchange=EXCHANGE, routing_key=own_queue_name, body=msg)

    if DEBUG:
        print(DEBUG_FLAG, 'sent:', msg)


'''
1- After receiving a request, pushing the request in its own request queue (ordered by time stamps) and reply with a time stamp.
2- After receiving release message, remove the corresponding request from its own request queue.
3- If own request is at the head of its queue and all replies have been received, enter critical section.
'''
def receive_data():
    def callback(ch, method, properties, body):
        global waiting_responses
        global requests
        global clock

        sender_name = method.routing_key

        # ignore own messages
        if sender_name == own_queue_name:
            if DEBUG:
                print(DEBUG_FLAG, "ignoring own message")
            return

        if DEBUG:
            print(DEBUG_FLAG, "received %r from %r" % (body, method.routing_key))

        # decode message
        decoded_msg = body.decode('UTF-8').split(MSG_SEPARATOR)
        msg_type = decoded_msg[0]
        msg_timestamp = int(decoded_msg[1])

        # recalculate timestamp
        clock = max(clock, msg_timestamp)
        increment_clock()

        if msg_type == MSG_REQUEST:
            print("client %s wants to enter in critical section" % (sender_name, ))

            # put received request in requests queue
            request = Request(msg_timestamp, sender_name)
            requests.put_nowait((request.timestamp, request))

            # answer request
            # WARNING: this get method is not threading safe
            smaller_timestamp, first_request = requests.queue[0]
            if smaller_timestamp == msg_timestamp:
                response_msg = MSG_RESPONSE_PERMISSION_GRANTED
            else:
                response_msg = MSG_RESPONSE_NO_PERMISSION_GRANTED


        elif msg_type == MSG_RELEASE:
            pass
        elif msg_type == MSG_RESPONSE:
            # after receiving all responses, stop waiting
            # TODO: only set boolean to False after receiving ALL responses
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
