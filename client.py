#!/usr/bin/env python
import queue
import threading
from time import sleep

import pika

from core import *

DEBUG = True

''' constants '''
BROADCAST = 'broadcast'
DEBUG_FLAG = '[DEBUG]'
MSG_REQUEST = 'REQUEST'
MSG_RELEASE = 'RELEASE'
MSG_RESPONSE_PERMISSION_GRANTED = 'GRANTED_PERMISSION'
MSG_RESPONSE_NO_PERMISSION_GRANTED = 'NO_PERMISSION'

# thread-safe requests queue
requests = queue.PriorityQueue()
clock = 0
# TODO: synchronize clocks when a new client arrives


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
    for i in range(0, seconds):
        sleep(1)
        print('work done: ' + str(int(100*(i/seconds))) + '%')
    print('EXIT critical section')


def increment_clock():
    global clock
    clock += 1
    if DEBUG:
        print(DEBUG_FLAG, "incremented clock to", clock)


'''
    # reply_to => the queue who triggered the request (and should receive the response)
    # correlation_id (string) => id of the request
'''


def send_msg(msg_type, routing_key, is_broadcast=False):
    exchange = BROADCAST if is_broadcast else ''
    props = pika.BasicProperties(reply_to=own_queue_name, correlation_id=str(clock),)
    channel.confirm_delivery()
    channel.basic_publish(exchange=exchange, routing_key=routing_key, body=msg_type, properties=props)
    if DEBUG:
        print(DEBUG_FLAG, '[SEND] msg: %s, timestamp: %s, routing_key: %s' % (msg_type, clock, routing_key))



def send_request(access_duration):
    global requests

    # increment timestamp before creating request
    increment_clock()

    # 1- push request to own queue
    request = Request(clock, own_queue_name, access_duration)
    requests.put_nowait((request.timestamp, request))

    # 2- broadcast request msg
    send_msg(MSG_REQUEST, own_queue_name, True)


'''
1- After receiving a request, pushing the request in its own request queue (ordered by time stamps) and reply with a time stamp.
2- After receiving release message, remove the corresponding request from its own request queue.
3- If own request is at the head of its queue and all replies have been received, enter critical section.
'''
def receive_data():
    def callback(ch, method, props, body):
        global waiting_responses
        global requests
        global clock

        sender_name = props.reply_to

        # ignore own messages
        if sender_name == own_queue_name:
            if DEBUG:
                print(DEBUG_FLAG, "[RECEIVE] ignoring own message")
            return

        # decode message
        msg_type = body.decode('UTF-8')
        msg_timestamp = int(props.correlation_id)

        if DEBUG:
            print(DEBUG_FLAG, "[RECEIVE] msg: %r, timestamp: %s, sender: %r" % (body, msg_timestamp, sender_name))

        # recalculate clock
        clock = max(clock, msg_timestamp)
        increment_clock()

        if msg_type == MSG_REQUEST:
            print("node %s wants to enter in critical section" % (sender_name, ))

            # put received request in requests queue
            request = Request(msg_timestamp, sender_name)
            requests.put_nowait((request.timestamp, request))

            # get first element from queue
            smaller_timestamp, first_request = requests.get_nowait()   # equivalent to get(False)

            # accept or refuse request
            if smaller_timestamp == msg_timestamp:
                response_msg = MSG_RESPONSE_PERMISSION_GRANTED
            else:
                response_msg = MSG_RESPONSE_NO_PERMISSION_GRANTED

            # put element back on queue
            requests.put_nowait((smaller_timestamp, first_request))

            # send response
            send_msg(response_msg, props.reply_to)

        elif msg_type == MSG_RELEASE:
            # remove first element from queue
            requests.get_nowait()

        elif msg_type == MSG_RESPONSE_PERMISSION_GRANTED:
            # after receiving all responses, stop waiting
            # TODO: only set boolean to False after receiving ALL responses
            waiting_responses = False

            if not waiting_responses:
                # get first element from queue
                smaller_timestamp, my_request = requests.get_nowait()

                enter_critical_section(my_request.access_duration)

                # warn other clients that the use of CS ended
                send_msg(MSG_RELEASE, own_queue_name, True)

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
            #if waiting_responses:
            #    print("Request ignored: waiting responses for last request")
            #    continue
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
    channel.exchange_declare(exchange=BROADCAST, exchange_type='fanout')

    # create own queue
    # exclusive tag is for deleting the queue when disconnecting
    result = channel.queue_declare(exclusive=True)
    own_queue_name = result.method.queue
    print("own queue name = %s", own_queue_name)

    # bind own queue to exchange
    channel.queue_bind(exchange=BROADCAST, queue=own_queue_name)

    run_threads()
