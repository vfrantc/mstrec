#!/usr/bin/env python3
import time
import json
import socket
import argparse
import threading
import logging
import queue

logging.basicConfig(level=logging.DEBUG, format='(%(threadName)-10s) %(message)s')


def _encode(msg):
    '''Converts dictionary to binary coded json-string which can be send over the network'''
    json_str = json.dumps(msg)
    return bytes(json_str, encoding='utf-8')


def _decode(msg):
    '''Decodes binary data to dictionary'''
    result = None
    try:
        result = json.loads(msg.decode("utf-8"))
    except Exception:
        pass
    return result


class Receiver(threading.Thread):
    '''1. Separate thread to open port to listen
       1. Recieves the message
       2. Decodes - messages are binary coded json strings, but we work with dictionaries
       3. If it is a heartbeat message - respnonds
       4. If it is not - puts dictionary into incoming queue to be fetched and processed by the node
       '''

    def __init__(self, host, port, queue):
        super().__init__()
        self.setName('Receiver')
        self._queue = queue

        # Open a socket and bind to the port
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.bind((host, port))
        self._sock.listen()

    def run(self):
        while True:
            # recieve messages and put them into queue
            conn, addr = self._sock.accept()
            data = conn.recv(1024)
            msg = _decode(data)
            logging.debug('msg: {}'.format(repr(msg)))

            if not msg:
                continue

            if msg['type'] == 'ping':
                '''respond with pong'''
                conn.sendall(_encode('{"type": "pong"}'))
                conn.close()
            else:
                msg['addr'] = addr
                incoming_queue.put(msg)


class Sender(threading.Thread):

    def __init__(self, incoming=None, outgoing=None):
        super().__init__()
        self.setName('Sender')
        self._incoming = incoming
        self._outgoing = outgoing
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def run(self):
        while True:
            if not self._queue.empty():
                try:
                    msg = self._queue.get()
                    logging.debug('msg: {}'.format(repr(msg)))
                    self._sock.connect((msg['host'], msg['port']))
                    self._sock.sendall(_encode(msg))
                except ConnectionRefusedError:
                    logging.debug('failure: {}'.format(msg['id']))
                    failure_msg = dict(type="fail", id=msg['id'])
                    self._incoming.put(failure_msg)
                finally:
                    self._sock.close()


class Communicator(object):
    def __init__(self, net_config=None, incoming=None, outgoing=None):
        self._config = net_config
        self._incoming = incoming
        self._outgoing = outgoing

    def send(self, id, msg):
        # translate id to addr:port and put in outgoing queue
        msg['host'] = self._config[id]['host']
        msg['port'] = self._config[id]['port']
        self._outgoing.put(msg)

    def ready(self):
        return not self._incoming.empty()

    def receive(self):
        msg = self._incoming.get()
        return msg


class Node(threading.Thread):
    '''Implements main reconfiguration functionality'''
    def __init__(self, id, links, edges, communicator):
        super().__init__()
        self.setName('Node')
        self.id = id
        self._ports = {i: nb_id for i, nb_id in enumerate(links)}
        self._edges = [self.port_to(edge) for edge in edges]
        self._con = communicator

        # State variables
        self.coord_so_far = None
        self.port_to_coord = None
        self.status = 'idle'
        self.recd_reply = {}

        self._heartbeat_timer = threading.Timer(1.0, self._heartbeat)
        self._expected_number_of_responses = len(self._ports)

    def _heart_beat(self):
        for edge_id in self._edges:
            logging.debug('sending heartbeat to {}'.format(edge_id))
            self._con.send(_encode(dict(type="ping")))

    def _on_reconfig(self, node_list, frag_id, from_id):
        '''
        sender_id = node_list[-1]
        if self.status == 'idle':
            if self.id in node_list:
                return
            if len(self.ports) == 1:
                self.get_node(sender_id).no_contention(self.id)
            self.coord_so_far = frag_id
            self.status = 'wait'
            self.port_to_coord = self.port_to(sender_id)
            for port in set(self.ports.keys()).difference([self.port_to(sender_id)]):
                self.get_node(self.ports[port]).reconfig(node_list + [self.id], frag_id)
        elif self.status == 'wait':
            e = self.port_to(sender_id)
            print(sender_id)
            print(e, '-', self.id)
            if self.id in node_list:
                self.get_node(self.ports[e]).no_contention(self.id)
                return

            if (frag_id == self.coord_so_far) and (e not in self.get_port()):
                self.get_node(self.ports[e]).no_contention(self.id)
                return

            # Resolve contention
            if (self.coord_so_far > frag_id) or ((self.coord_so_far == frag_id) and (self.id > sender_id)):
                self.get_node(sender_id).stop(self.coord_so_far, from_id=self.id)
            else:
                self.coord_so_far = frag_id
                self.get_node(self.ports[self.port_to_coord]).stop(frag_id, from_id=self.id)
                self.port_to_coord = self.port_to(sender_id)
        '''


    def _on_stop(self, frag_id, from_id):
        '''
        if self.status == 'idle':
            return
        print('Node {} recieved stop({}, {})'.format(self.id, frag_id, from_id))
        p = self.port_to(from_id)
        if frag_id > self.coord_so_far:
            self.coord_so_far = frag_id
            self.get_node(self.ports[self.port_to_coord]).stop(frag_id, from_id=self.id)
            self.port_to_coord = p
        if frag_id == self.coord_so_far:
            if self.port_to_coord not in self.get_port():
                self.get_node(self.ports[self.port_to_coord]).no_contention(self.id)
                self.recd_reply[self.port_to_coord] = 'no_contention'
            else:
                self.get_node(self.ports[self.port_to_coord]).stop(frag_id, from_id=self.id)
            self.port_to_coord = p
        if frag_id < self.coord_so_far:
            self.get_node(self.ports[p]).stop(frag_id, from_id=self.id)
        '''

    def _on_everybody_responded(self):
        '''
        if 'accepted' in self.recd_reply.values():
            self.get_node(self.ports[self.port_to_coord]).accepted(self.id)
            if self.port_to_coord not in self.get_port():
                self.assign_edge(self.port_to_coord)
        else:
            if (self.port_to_coord not in self.get_port()) and len(self.set_of_ports().difference([self.port_to_coord]).intersection(self.get_port()))!=0:
                    self.get_node(self.ports[self.port_to_coord]).accepted(self.id)
                    self.assign_edge(self.port_to_coord)
            else:
                self.get_node(self.ports[self.port_to_coord]).no_contention(self.id)
        '''
        # Go back to the idle state
        self.recd_reply = {}
        self.status = 'idle'
        self.coord_so_far = None
        self.port_to_coord = None

    def port_to(self, node_id):
        for port_idx, cur_id in self._ports.items():
            if cur_id == node_id:
                return port_idx
        return None

    def set_of_ports(self):
        return set(self._ports.keys())

    def assign_edge(self, port_id):
        self._edges.append(port_id)

    def get_port(self):
        return self._edges

    def run(self):
        while True:
            if not incoming_queue.empty():
                msg = incoming_queue.get()
                logging.debug('Processing message: {}'.format(repr(msg)))

                if msg.type == 'start':
                    print('Activating')
                    time.sleep(1.0)
                    self._heartbeat_timer.start()
                elif msg.type == 'reconfig':
                    self._on_reconfig(msg.node_list, msg.frag_id, msg.id)
                elif msg.type == 'no_contention':
                    self.recd_reply[self.port_to(msg.id)] = 'no_contention'
                    if len(self.recd_reply) == self._expected_number_of_responses:
                        self._on_everybody_responded()
                elif msg.type == 'accept':
                    self.recd_reply[self.port_to(msg.from_id)] = 'accepted'
                    if len(self.recd_reply) == self._expected_number_of_responses:
                        self._on_everybody_responded()
                elif msg.type == 'stop':
                    self._on_stop(msg.frag_id, msg.id)
                elif msg.type == 'fail':
                    self._expected_number_of_responses = len(self._ports)
                    # send reconfiguration request through all the ports
                    for dest_id in self._ports.values():
                        self._con.send(id=dest_id,
                                       msg=dict(type='reconfig',
                                                node_list=[self.id],
                                                frag_id=self.id))


def opt_parser():
    parser = argparse.ArgumentParser(description='Network reconfiguration node')
    parser.add_argument('--id',
                        default="0",
                        type=str)
    parser.add_argument('--wait',
                        default=10,
                        type=int,
                        help='Time to wait before we start to send heartbit')
    parser.add_argument('--net_config', default='sample_graph.json', type=str)
    return parser


def load_config(fname):
    with open(fname, 'r') as fd:
        return json.load(fd)


if __name__ == '__main__':
    # Parse command line arguments
    parser = opt_parser()
    opt = parser.parse_args()

    # Load network configuration
    net_config = load_config(opt.net_config)

    # Get configuration of this node
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
                                outgoing=outgoing_queue)

    # thread to receive incoming messages
    receiver = Receiver(host=host, port=port,
                        queue=incoming_queue)
    sender = Sender(queue=outgoing_queue)
    node = Node(id=my_id,
                links=links,
                edges=edges,
                communicator=communicator)

    receiver.start()
    sender.start()
    node.start()

    receiver.join()
    sender.join()
    node.join()