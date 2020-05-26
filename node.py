#!/usr/bin/env python3
import time
import socket
import argparse
import threading
import queue
import logging
logging.basicConfig(level=logging.DEBUG, format='(%(threadName)-10s) %(message)s')

from common import encode
from common import decode
from common import load_config
from common import send_msg
from common import there_is_a_none


class Receiver(threading.Thread):
    '''Separate thread to open port to listen
       1. Recieves the message
       2. Decodes - messages are binary coded json strings, but we work with dictionaries
       3. If it is a heartbeat message - respnonds
       4. If it is not - puts dictionary into incoming queue to be fetched and processed by the node
       '''

    def __init__(self, host, port, queue):
        super().__init__()
        self.setName('Receiver')
        # Queue for incoming messages
        self._queue = queue

        # Open a socket and bind to the port
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.bind((host, port))
        self._sock.listen()

    def run(self):
        while True:
            # recieve messages and put them into queue
            conn, addr = self._sock.accept()
            conn.settimeout(1)
            data = conn.recv(1024)

            msg = decode(data)
            #logging.debug('msg: {}'.format(repr(msg)))

            if not msg:
                continue

            if msg['type'] == 'ping':
                # respond with pong to heartbeat messages
                # no need to put it into queue
                #logging.debug('Reacting to heartbeat from {}'.format(msg['id']))
                conn.sendall(encode('{"type": "pong"}'))
            else:
                incoming_queue.put(msg)
            conn.close()


class Sender(threading.Thread):
    '''Sender thread. Fetches messages from the outgoing queue and send them one by one.
       Also initiates 'failure mode' if unable to send failure message.
    '''

    def __init__(self, incoming=None, outgoing=None):
        super().__init__()
        self.setName('Sender')
        self._incoming = incoming
        self._outgoing = outgoing
        self._sock = None

    def run(self):
        while True:
            if not self._outgoing.empty():
                msg = self._outgoing.get()
                self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                try:
                    self._sock.connect((msg['host'], msg['port']))
                    self._sock.sendall(encode(msg))
                except ConnectionRefusedError:
                    if msg['type'] == 'ping':
                        #logging.debug('failure: {}'.format(msg['to_id']))
                        failure_msg = dict(type="fail", id=msg['to_id'])
                        self._incoming.put(failure_msg)
                finally:
                    self._sock.close()


class Communicator(object):
    '''Proxy class. Translates network-wide ids to actual ip-addresses and ports'''
    def __init__(self, net_config=None, incoming=None, outgoing=None, id=None):
        # network configuration as a dictionary node_id -> ('port', 'host')
        self._config = net_config
        self._incoming = incoming
        self._outgoing = outgoing
        self._id = id

    def send(self, id, msg):
        # translate id to addr:port and put in outgoing queue
        msg['host'] = self._config[id]['host']
        msg['port'] = self._config[id]['port']

        # add to_id and from_id to the message
        msg['to_id'] = id
        msg['id'] = self._id

        # schedule message for sending
        self._outgoing.put(msg)

    def ready(self):
        # check if queue contains any messages
        return not self._incoming.empty()

    def receive(self):
        # get message from incoming queue, one at the time
        return self._incoming.get()


class Node(threading.Thread):
    '''Implements main reconfiguration functionality'''

    def __init__(self, id, links, edges, communicator):
        super().__init__()
        self.setName('Node')

        self.id = id

        # hardware link id -> networkwide logical node id
        self._ports = {i: nb_id for i, nb_id in enumerate(links)}
        self._edges = [self.port_to(edge) for edge in edges]
        self._con = communicator

        # state variables
        self.coord_so_far = None
        self.port_to_coord = None
        self.status = 'idle'
        self.recd_reply = {}

    def _heartbeat(self):
        threading.Timer(10.0, self._heartbeat).start()

        for port_id in self._edges:
            node_id = self._ports[port_id]
            # logging.debug('sending heartbeat to {}'.format(node_id))
            self._con.send(node_id, dict(type="ping"))


    def port_to(self, node_id):
        for port_idx, cur_id in self._ports.items():
            if cur_id == node_id:
                return port_idx
        return None

    def set_of_ports(self):
        return set(self._ports.keys())

    def assign_edge(self, port_id):
        self._edges.append(port_id)

    def remove_edge(self, node_id):
        port_id = self.port_to(node_id)
        if port_id in self._edges:
            self._edges = list(set(self._edges).difference([port_id]))

    def get_port(self):
        return self._edges

    def _on_everybody_responded(self):
        responses = list(self.recd_reply.values())
        if 'accept' in responses:
            self._con.send(self._ports[self.port_to_coord],
                           dict(type='accepted'))
        else:
            if self.port_to_coord is not None:
                self._con.send(self.coord_so_far, dict(type='no_contention'))

    def run(self):
        while True:
            if not incoming_queue.empty():
                msg = incoming_queue.get()
                logging.debug('Processing message: {}\n'
                              'Current recvd: {}\n'
                              'Current status: {}\n'
                              'Coord so far: {}\n'
                              'Port to coord: {}'.format(repr(msg),
                                                         self.recd_reply,
                                                         self.status,
                                                         self.coord_so_far,
                                                         self.port_to_coord))
                if msg['type'] == 'start':
                    time.sleep(2.0)
                    self._heartbeat()
                elif msg['type'] == 'reconfig':
                    sender_id = int(msg['id'])
                    frag_id = int(msg['frag_id'])
                    if self.status == 'idle':
                        self.status = 'wait'
                        failed_id = int(msg['failed_node'])
                        self.coord_so_far = sender_id
                        self.port_to_coord = self.port_to(sender_id)

                        can_communicate = list(set(self._ports.values()).difference([sender_id, failed_id]))
                        if len(can_communicate) == 0:
                            self._con.send(sender_id, dict(type='no_contention'))
                        else:
                            for node_id in can_communicate:
                                self.recd_reply[node_id] = None
                                self._con.send(node_id, dict(type='reconfig',
                                                             node_list=msg['node_list'] + [self.id],
                                                             frag_id=msg['frag_id'],
                                                             failed_node=failed_id))
                    else:
                        e = self.port_to(sender_id)

                        # Message is the copy of message recieved earlier
                        if (frag_id == self.coord_so_far) and (e not in self.get_port()):
                            logging.debug('Message was recieved earlier')
                            self._con.send(self._ports[e], dict(type='no_contention'))
                            continue

                        # Detected loop
                        if self.id in msg['node_list']:
                            logging.debug('Loop is detected')
                            self._con.send(self._port[e], dict(type='no_contention'))
                            continue

                        # Resolve contention
                        if (self.coord_so_far > frag_id) or ((self.coord_so_far == frag_id) and (self.id > sender_id)):
                            self._con.send(sender_id, dict(type='stop', frag_id=self.coord_so_far))
                        else:
                            self.coord_so_far = frag_id
                            if self.port_to_coord is not None:
                                self._con.send(self._ports[self.port_to_coord], dict(type='stop', frag_id=frag_id))
                            self.port_to_coord = self.port_to(sender_id)
                elif msg['type'] == 'no_contention':
                    sender_id = int(msg['id'])
                    if sender_id in self.recd_reply.keys():
                        self.recd_reply[sender_id] = 'no_contention'
                    if not there_is_a_none(self.recd_reply):
                        self._on_everybody_responded()
                elif msg['type'] == 'accept':
                    sender_id = int(msg['id'])
                    if sender_id in self.recd_reply.keys():
                        self.recd_reply[sender_id] = 'accept'
                    if not there_is_a_none(self.recd_reply):
                        self._on_everybody_responded()
                elif msg['type'] == 'stop':
                    pass
                elif msg['type'] == 'fail':
                    failed_node = int(msg['id'])
                    self.remove_edge(failed_node)
                    self.status = 'wait'
                    self.coord_so_far = self.id
                    self.port_to_coord = None
                    for node_id in set(self._ports.values()).difference([failed_node]):
                        self.recd_reply[node_id] = None
                        self._con.send(node_id, dict(type='reconfig', node_list=[self.id], frag_id=self.id, failed_node=failed_node))
                elif msg['type'] == 'extract':
                    host = msg['host']
                    port = msg['port']
                    graph_msg = encode(dict(id=self.id,
                                            links=list(self._ports.values()),
                                            edges=[self._ports[port_id] for port_id in self._edges]))

                    send_msg(msg=graph_msg, host=host, port=port)


def opt_parser():
    parser = argparse.ArgumentParser(description='Network reconfiguration node')
    parser.add_argument('--id',
                        default=10,
                        type=int)
    parser.add_argument('--net_config', default='config/sample_graph3.json', type=str)
    return parser

if __name__ == '__main__':
    # Parse command line arguments
    parser = opt_parser()
    opt = parser.parse_args()

    # Load network configuration
    net_config = load_config(opt.net_config)

    # Get configuration of this node
    print(net_config[opt.id])
    my_id = opt.id
    host = net_config[my_id]['host']
    port = net_config[my_id]['port']
    links = net_config[my_id]['links']
    edges = net_config[my_id]['edges']

    # create queues for incoming and outgoig messages
    incoming_queue = queue.Queue()
    outgoing_queue = queue.Queue()

    communicator = Communicator(net_config=net_config,
                                incoming=incoming_queue,
                                outgoing=outgoing_queue,
                                id=my_id)

    # thread to receive incoming messages
    receiver = Receiver(host=host,
                        port=port,
                        queue=incoming_queue)
    sender = Sender(incoming=incoming_queue,
                    outgoing=outgoing_queue)
    node = Node(id=my_id,
                links=links,
                edges=edges,
                communicator=communicator)

    receiver.start()
    sender.start()
    node.start()

    node.join()