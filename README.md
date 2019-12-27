# Lamport's Algorithm Implementation

That's a distributed implementation of Lamport's Algorithm for Mutual Exclusion, written in python 3 using pika rabbitMQ library.

Even if the nodes are launched in the same machine, they receive different ports.

The system simulates a situation where multiple processes (from different machines or not) try to access a shared resource, that may only be used by one node at a time.

Full documentation at: [Repport]

## Requirements

| Requirement | Function |
| ------ | ------ |
| Python 3 | This implementation was tested in Python 3.6.2 so any Python 3 compiler may be able to run the code. |
| Pika 0.11.0 | RabbitMQ python library |
| RabbitMQ 3.6.12 | You must run a RabbitMQ Server on localhost |

## Usage

There are some usage examples on the [Repport], but, to start:

1- Install [RabbitMQ] and run a server on localhost.

2- Install dependecies
```sh
pip3 install -r requirements.txt
```

3- Launch a node:
```sh
python node.py
```

4- Broadcast request message:

After the node is connected, type a integer x to create a request of x seconds.



### Limitations

This implementation has the following limitations, that may be respected to avoid deadlock situations.

1- At least two clients must be running before the first request is sent.

2- A node can only join the system if the queues of all the others nodes are empty.

3- No node shall leave the system.


### Silent Mode

To hide debug logs so the terminal only shows the critical section being used, open node.py and change the global constant DEBUG to False.

[Repport]: <https://drive.google.com/open?id=1IJca8i3aw37f202tzBq5syTYBI5Qfsq1QPfp_DXd6BA>
[RabbitMQ]: <https://www.rabbitmq.com/>
