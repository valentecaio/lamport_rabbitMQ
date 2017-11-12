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

# thread-safe requests queue
requests = queue.Queue()


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
    global requests

    # 1- push request to own queue
    request = Request(request_time, own_queue_name)
    requests.put_nowait(request)

    # 2- broadcast request msg
    msg = MSG_REQUEST + ' ' + str(request_time)
    channel.basic_publish(exchange=EXCHANGE, routing_key=own_queue_name, body=msg)

    if DEBUG:
        print(DEBUG_FLAG, 'request sent')


'''
1- After receiving a request, pushing the request in its own request queue (ordered by time stamps) and reply with a time stamp.
2- After receiving release message, remove the corresponding request from its own request queue.
3- If own request is at the head of its queue and all replies have been received, enter critical section.
'''
def receive_data():
    def callback(ch, method, properties, body):
        global waiting_responses
        global requests

        sender_name = method.routing_key

        # ignore own messages
        if sender_name == own_queue_name:
            if DEBUG:
                print(DEBUG_FLAG, "ignoring own message")
            return

        if DEBUG:
            print(DEBUG_FLAG, "received %r from %r" % (body, method.routing_key))

        # decode and treat message
        decoded_msg = body.decode('UTF-8').split()
        if decoded_msg[0] == MSG_REQUEST:
            print("client %s wants to enter in critical section" % (sender_name, ))
            # put received request in requests queue
            requests.put_nowait(Request(int(decoded_msg[1]), sender_name))

            # answer request
            # WARNING: this get method is not threading safe
            first_req = requests[0]
            if first_req.owner_name == sender_name:
                response_msg = MSG_RESPONSE_PERMISSION_GRANTED
            else:
                response_msg = MSG_RESPONSE_NO_PERMISSION_GRANTED


        elif decoded_msg[0] == MSG_RELEASE:
            pass
        elif decoded_msg[0] == MSG_RESPONSE:
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
