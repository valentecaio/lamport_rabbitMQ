#!/usr/bin/env python
import queue
import threading
from time import sleep

import pika

try:
    from pprint import pprint
except:
    pprint = print

from core import *

DEBUG = True

''' constants '''
BROADCAST = 'broadcast'
DEBUG_FLAG = '[DEBUG]'
MSG_REQUEST = 'REQUEST'
MSG_RELEASE = 'RELEASE'
MSG_PERMISSION_GRANTED = 'GRANTED_PERMISSION'
MSG_NETWORK_LENGTH_REQUEST = 'NETWORK_LENGTH'
MSG_NETWORK_LENGTH_ACK = 'NETWORK_LENGTH_ACK'


# thread-safe requests queue
requests = queue.PriorityQueue()
clock = 0
network_length = 1
received_permissions = 0

own_queue_name = ''
waiting_responses = False


'''
Requesting process

    1- Pushing its request in its own queue (ordered by time stamps)
    2- Sending a request to every node.
    3- Waiting for replies from all other nodes.
    4- If own request is at the head of its queue and all replies have been received, enter critical section.
    5- Upon exiting the critical section, remove its request from the queue and send a release message to every process.
'''


def simulate_critical_section_usage(seconds):
    print('ENTER critical section for', seconds, 'seconds')
    for i in range(0, seconds):
        sleep(1)
        print('work done: ' + str(int(100*(i/seconds))) + '%')
    print('EXIT critical section')


def increment_clock():
    global clock
    clock += 1
    if DEBUG:
        print(DEBUG_FLAG, "[CLOCK] incremented clock to", clock)


def requests_put(request):
    requests.put_nowait(request)
    print(DEBUG_FLAG, '[PUT]', request)
    pprint(requests.queue)


def requests_get():
    req = requests.get()  # equivalent to get(False)
    print(DEBUG_FLAG, '[GET]', req)
    pprint(requests.queue)
    return req

'''
    # reply_to => the queue who triggered the request (and should receive the response)
    # timestamp (int) => timestamp of the request
'''


def send_msg(msg_type, routing_key, is_broadcast=False):
    exchange = BROADCAST if is_broadcast else ''
    props = pika.BasicProperties(reply_to=own_queue_name, timestamp=clock,)
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
    requests_put(request)

    # 2- broadcast request msg
    send_msg(MSG_REQUEST, own_queue_name, True)


# process next element in the queue
def process_next_element():
    if requests.empty():
        return

    req = requests_get()
    # if node is the owner, enter in CS, else, send permission
    if req.owner_name == own_queue_name:
        if received_permissions == (network_length - 1):
            use_critical_section(req)
        else:
            requests_put(req)
    else:
        send_msg(MSG_PERMISSION_GRANTED, req.owner_name)
        requests_put(req)


def use_critical_section(request):
    global waiting_responses
    global received_permissions

    waiting_responses = False
    received_permissions = 0

    simulate_critical_section_usage(request.access_duration)

    # warn other clients that the use of CS is over
    send_msg(MSG_RELEASE, own_queue_name, True)

    process_next_element()


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
        global network_length
        global received_permissions

        sender_name = props.reply_to

        # ignore own messages
        if sender_name == own_queue_name:
            return

        # decode message
        msg_type = body.decode('UTF-8')
        msg_timestamp = props.timestamp

        if DEBUG:
            print(DEBUG_FLAG, "[RECEIVE] msg: %r, timestamp: %s, sender: %r" % (body, msg_timestamp, sender_name))

        # recalculate clock
        clock = max(clock, msg_timestamp)
        increment_clock()

        if msg_type == MSG_REQUEST:
            # put received request in requests queue
            request = Request(msg_timestamp, sender_name)
            requests_put(request)

            # get first element from queue
            first_request = requests_get()

            # accept or refuse request
            if first_request.timestamp == msg_timestamp:
                response_msg = MSG_PERMISSION_GRANTED
                # send response
                send_msg(response_msg, props.reply_to)

            # put element back on queue
            requests_put(first_request)

        elif msg_type == MSG_RELEASE:
            # remove first element from queue, since it was released by its owner
            if not requests.empty():
                requests_get()

            process_next_element()

        elif msg_type == MSG_NETWORK_LENGTH_REQUEST:
            network_length += 1
            print("[NETWORK] New client on the system:", network_length, 'clients')
            # respond with ack
            send_msg(MSG_NETWORK_LENGTH_ACK, props.reply_to)

        elif msg_type == MSG_NETWORK_LENGTH_ACK:
            network_length += 1

        elif msg_type == MSG_PERMISSION_GRANTED:
            # after receiving all responses, stop waiting
            received_permissions += 1
            print(DEBUG_FLAG, '[PERMISSION]', received_permissions)

            if received_permissions == (network_length-1):
                # get first element from queue and check if its correct before continuing
                req = requests_get()
                if req.owner_name == own_queue_name:
                    use_critical_section(req)

    # start to listen to messages
    channel.basic_consume(callback, queue=own_queue_name, no_ack=True)
    channel.start_consuming()


# used by user interface thread
def read_keyboard():
    global waiting_responses

    print("Type requests to send: \t")

    while 1:
        try:
            user_input = input("")
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
    # start connection
    print('Starting connection')
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

    # request network length
    send_msg(MSG_NETWORK_LENGTH_REQUEST, own_queue_name, is_broadcast=True)

    run_threads()
