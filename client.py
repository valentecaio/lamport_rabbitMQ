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

# set as False to hide logs
DEBUG = True

''' constants '''
DEBUG_FLAG = '[DEBUG]'                          # flag used in logs
BROADCAST = 'broadcast'                         # id of exchange common to all nodes (used as broadcast)
MSG_REQUEST = 'REQUEST'                         # Lamport request message prefix
MSG_RELEASE = 'RELEASE'                         # Lamport release message prefix
MSG_PERMISSION = 'PERMISSION'                   # Lamport granted permission message prefix
MSG_NETWORK_SIZE_REQUEST = 'NETWORK_SIZE'       # network size request message prefix
MSG_NETWORK_SIZE_ACK = 'NETWORK_SIZE_ACK'       # network size response message prefix


''' global variables '''
requests = queue.PriorityQueue()                # thread-safe requests queue, automatically ordered by timestamps
clock = 0                                       # logical clock used by Lamport Algorithm
network_size = 1                                # number of nodes in the system
received_permissions = 0                        # global counter: number of received permissions for the actual request
node_id = ''                                    # identifier of this node in the system


# simulate a critical section usage;
# it can be replaced by any function that really uses a critical section;
def simulate_critical_section_usage(seconds):
    print('ENTER critical section for', seconds, 'seconds')
    for i in range(0, seconds):
        sleep(1)
        print('work done: ' + str(int(100*(i/seconds))) + '%')
    print('EXIT critical section')


# increment global variable clock
def increment_clock():
    global clock
    clock += 1
    if DEBUG:
        print(DEBUG_FLAG, "[CLOCK] incremented clock to", clock)


# return True if, and only if, all the necessary permissions for the last request have been received
def node_has_permissions():
    return received_permissions == (network_size-1)


# put a request in node's request queue
def requests_put(request):
    requests.put_nowait(request)
    print(DEBUG_FLAG, '[PUT]', request)
    pprint(requests.queue)


# get the first request from node's request queue
def requests_get():
    req = requests.get()  # equivalent to get(False)
    print(DEBUG_FLAG, '[GET]', req)
    pprint(requests.queue)
    return req


# send a message where the body is msg_type;
# if the message is_broadcast, the message is sent in broadcast and routing_key is ignored;
# else, the message is sent individually and routing_key should be the receiver id;
def send_msg(msg_type, routing_key, is_broadcast=False):
    # timestamp (int)   => the clock that the sender had in the moment it sent the message
    # reply_to (string) => the id of the sender
    props = pika.BasicProperties(reply_to=node_id, timestamp=clock,)

    channel.confirm_delivery()  # TODO: need to test this line

    exchange = BROADCAST if is_broadcast else ''
    channel.basic_publish(exchange=exchange, routing_key=routing_key, body=msg_type, properties=props)
    if DEBUG:
        print(DEBUG_FLAG, '[SEND] msg: %s, timestamp: %s, routing_key: %s' % (msg_type, clock, routing_key))


# trigger a request according to the steps of Lamport algorithm
def send_request(access_duration):
    # increment timestamp before creating a request
    increment_clock()

    # push request to own queue
    request = Request(clock, node_id, access_duration)
    requests_put(request)

    # broadcast request msg
    send_msg(MSG_REQUEST, node_id, True)


# infinite looping to read keyboard inputs and treat them as requests
def read_keyboard():
    print("Type requests to send: \t")
    while 1:
        try:
            user_input = input("")
            request_time = int(user_input)
            # send request and wait for responses
            send_request(request_time)
        except ValueError:
            print("Your request should be an integer")
            continue


# process next request in node's queue;
# if node is not the owner, send permission to owner;
# if node is the owner and all permissions have been received, enter in CS;
# else, put element again in queue and wait for permissions;
def process_next_request():
    # if there's no element to process, job is done
    if requests.empty():
        return

    req = requests_get()

    # node is not the owner
    if req.owner_id != node_id:
        send_msg(MSG_PERMISSION, req.owner_id)
        requests_put(req)

    # node is the owner and has all permissions
    elif node_has_permissions():
        enter_critical_section(req)

    # there are some permissions missing, wait for them
    else:
        requests_put(req)


# this function must only be called after receiving all permissions for a request;
# the received_permissions global counter is reset to zero;
# the thread enter in critical section and a release message is broadcast after finishing the usage of CS;
# then, the following request is processed;
def enter_critical_section(request):
    global received_permissions
    received_permissions = 0

    simulate_critical_section_usage(request.access_duration)

    # warn other nodes that the use of CS is over
    send_msg(MSG_RELEASE, node_id, True)

    process_next_request()


# callback for main channel basic_consume
# decode and process a received message
def treat_received_data(ch, method, props, body):
    global clock
    global network_size
    global received_permissions

    # decode message attributes
    sender_id, msg_type, msg_timestamp = props.reply_to, body.decode, props.timestamp

    # ignore own broadcast messages
    if sender_id == node_id:
        return

    if DEBUG:
        print(DEBUG_FLAG, "[RECEIVE] msg: %r, timestamp: %s, sender: %r" % (body, msg_timestamp, sender_id))

    # recalculate clock, according to Lamport Algorithm
    clock = max(clock, msg_timestamp)
    increment_clock()

    # treat messages differently according to their types
    if msg_type == MSG_REQUEST:
        # put received request in requests queue
        requests_put(Request(msg_timestamp, sender_id))

        # get first element from queue
        req = requests_get()

        # if the received request is the first request in the queue, send a permission to its owner
        if req.timestamp == msg_timestamp:
            send_msg(MSG_PERMISSION, sender_id)

        # put element back on queue
        requests_put(req)

    elif msg_type == MSG_RELEASE:
        # remove first element from queue, since it was released by its owner
        if not requests.empty():
            requests_get()

        process_next_request()

    elif msg_type == MSG_NETWORK_SIZE_REQUEST:
        # a new node joined the system, add it to the network size counter
        network_size += 1
        print("[NETWORK] New node on the system:", network_size, 'nodes')

        # answer message with ack
        send_msg(MSG_NETWORK_SIZE_ACK, sender_id)

    elif msg_type == MSG_NETWORK_SIZE_ACK:
        # someone answered the request, add it to the network size counter
        network_size += 1

    elif msg_type == MSG_PERMISSION:
        # after receiving all permissions, stop waiting
        received_permissions += 1
        print(DEBUG_FLAG, '[PERMISSION]', received_permissions)

        if node_has_permissions():
            # if the first request in the queue is from this node, process it
            # else, ignore it and keep waiting for a release
            req = requests_get()
            if req.owner_id == node_id:
                enter_critical_section(req)
            else:
                requests_put(req)


if __name__ == '__main__':
    # start connection
    print('Starting connection')
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    # create own queue
    # exclusive tag is for deleting the queue when disconnecting
    result = channel.queue_declare(exclusive=True)
    node_id = result.method.queue
    print("own queue name = %s", node_id)

    # connect to exchange broadcast
    channel.exchange_declare(exchange=BROADCAST, exchange_type='fanout')
    # bind own queue to exchange, so that node can received broadcast messages
    channel.queue_bind(exchange=BROADCAST, queue=node_id)

    # create thread to read user inputs
    thread_input = threading.Thread(target=read_keyboard)
    thread_input.daemon = True
    thread_input.start()

    # calculate network size before creating any request
    send_msg(MSG_NETWORK_SIZE_REQUEST, node_id, is_broadcast=True)

    # start to listen to messages
    channel.basic_consume(treat_received_data, queue=node_id, no_ack=True)
    channel.start_consuming()
