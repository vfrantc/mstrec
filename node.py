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
            logging.debug('msg: {}'.format(repr(msg)))

            if not msg:
                continue

            if msg['type'] == 'ping':
                # respond with pong to heartbeat messages
                # no need to put it into queue
                # TODO: Is this a potential problem???
                logging.debug('Reacting to heartbeat from {}'.format(msg['id']))
                conn.sendall(encode('{"type": "pong"}'))
            else:
                # TODO: Check if we actually need this information
                msg['addr'] = addr
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
                logging.debug('msg: {}'.format(repr(msg)))
                # TODO: Is it possible to reuse the socket???
                self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                try:
                    self._sock.connect((msg['host'], msg['port']))
                    self._sock.sendall(encode(msg))
                except ConnectionRefusedError:
                    if msg['type'] == 'ping':
                        logging.debug('failure: {}'.format(msg['to_id']))
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
            logging.debug('sending heartbeat to {}'.format(node_id))
            self._con.send(node_id, dict(type="ping"))

    def _on_reconfig(self, node_list, frag_id, from_id):
        sender_id = node_list[-1]
        if self.status == 'idle':
            logging.debug('Reconfig, was in idle state!!')
            if len(self._ports) == 1:
                # TODO: This is wrong. Should check not the number of ports, but 'available' ports. But this probably not the problem.
                self._con.send(sender_id, dict(type='no_contention'))
            self.coord_so_far = frag_id
            self.status = 'wait'
            self.port_to_coord = self.port_to(sender_id)
            for port in set(self._ports.keys()).difference([self.port_to(sender_id)]):
                self._con.send(self._ports[port], dict(type='reconfig',
                                                       node_list=node_list+[self.id],
                                                       frag_id=frag_id))
        elif self.status == 'wait':
            logging.debug('Reconfig, was in wait state!!')
            e = self.port_to(sender_id)

            if (frag_id == self.coord_so_far) and (e not in self.get_port()):
                logging.debug('This node is the initiator of the reconfig: no_contention!!')
                self._con.send(self._ports[e], dict(type='no_contention'))
                return

            if self.id in node_list:
                logging.debug('Already in the node_list: no_contention!!')
                self._con.send(self._ports[e], dict(type='no_contention'))
                return

            # Resolve contention
            if (self.coord_so_far > frag_id) or ((self.coord_so_far == frag_id) and (self.id > sender_id)):
                logging.debug('This node is the boss: stop with our frag_id!!')
                self._con.send(sender_id, dict(type='stop', frag_id=self.coord_so_far))
            else:
                logging.debug('This node is the slave: change coord_so_far!!')
                logging.debug('self._ports: {}'.format(repr(self._ports)))
                logging.debug('self.port_to_coord: {}'.format(self.port_to_coord))
                self.coord_so_far = frag_id
                # TODO: Is this an issue???
                self._con.send(self._ports[self.port_to_coord], dict(type='stop', frag_id=frag_id))
                self.port_to_coord = self.port_to(sender_id)

    def _on_stop(self, frag_id, from_id):
        p = self.port_to(from_id)
        if frag_id > self.coord_so_far:
            self.coord_so_far = frag_id
            self._con.send(self._ports[self.port_to_coord], dict(type='stop', frag_id=frag_id))
            self.port_to_coord = p
        if frag_id == self.coord_so_far:
            if self.port_to_coord not in self.get_port():
                self._con.send(self._ports[self.port_to_coord], dict(type='no_contention'))
                self.recd_reply[self.port_to_coord] = 'no_contention'
            else:
                self._con.send(self._ports[self.port_to_coord], dict(type='stop', frag_id=frag_id))
            self.port_to_coord = p
        if frag_id < self.coord_so_far:
            self._con.send(self._ports[p], dict(type='stop', frag_id=self.coord_so_far))


    def _on_everybody_responded(self):
        logging.debug('Everybody responded was actually called!!')
        logging.debug('Everybody responded: self.port_to_coord {} {}'.format(self.port_to_coord, self.status))
        if 'accepted' in self.recd_reply.values():
            self._con.send(self._ports[self.port_to_coord], dict(type='accepted'))
            if self.port_to_coord not in self.get_port():
                self.assign_edge(self.port_to_coord)
        else:
            # TODO: There might be an issue here
            if (self.port_to_coord not in self.get_port()) and len(self.set_of_ports().difference([self.port_to_coord]).intersection(self.get_port()))!=0:
                    print('!'*100)
                    self._con.send(self._ports[self.port_to_coord], dict(type='accepted'))
                    self.assign_edge(self.port_to_coord)
            else:
                self._con.send(self._ports[self.port_to_coord], dict(type='no_contention'))

        # Go back to the idle state
        self.recd_reply = {}
        self.status = 'idle'
        self.coord_so_far = None
        self.port_to_coord = None

    def port_to(self, node_id):
        for port_idx, cur_id in self._ports.items():
            if cur_id == node_id:
                return port_idx
        logging.debug('='*1000)
        return None

    def set_of_ports(self):
        return set(self._ports.keys())

    def assign_edge(self, port_id):
        logging.debug('Assign new edge to port: {}')
        logging.debug('Edges before: {}'.format(repr(self._edges)))
        self._edges.append(port_id)
        logging.debug('Edges after: {}'.format(repr(self._edges)))


    def remove_edge(self, node_id):
        port_id = self.port_to(node_id)
        logging.debug('Removing edge to node {} (port {})'.format(node_id, port_id))
        logging.debug('Edges before: {}'.format(repr(self._edges)))
        if port_id in self._edges:
            logging.debug('Removing edge to node {} (port {})'.format(node_id, port_id))
            self._edges = list(set(self._edges).difference([port_id]))
        logging.debug('Edges after: {}'.format(repr(self._edges)))

    def get_port(self):
        return self._edges

    def run(self):
        while True:
            if not incoming_queue.empty():
                msg = incoming_queue.get()
                logging.debug('Processing message: {}'.format(repr(msg)))

                if msg['type'] == 'start':
                    print('Activating in a second')
                    time.sleep(2.0)
                    self._heartbeat()
                elif msg['type'] == 'reconfig':
                    self.recd_reply[self.port_to(int(msg['id']))] = 'no_contention'
                    self._on_reconfig(msg['node_list'], int(msg['frag_id']), int(msg['id']))
                elif msg['type'] == 'no_contention':
                    if self.status == 'wait':
                        self.recd_reply[self.port_to(int(msg['id']))] = 'no_contention'
                        if len(self.recd_reply) == len(self._ports):
                            self._on_everybody_responded()
                elif msg['type'] == 'accept':
                    if self.status == 'wait':
                        self.recd_reply[self.port_to(int(msg['id']))] = 'accepted'
                        if len(self.recd_reply) == len(self._ports):
                            self._on_everybody_responded()
                elif msg['type'] == 'stop':
                    if self.status == 'wait':
                        self._on_stop(int(msg['frag_id']), int(msg['id']))
                elif msg['type'] == 'fail':
                    self.remove_edge(int(msg['id']))
                    # send reconfiguration request through all the ports
                    for dest_id in set(self._ports.values()).difference([int(msg['id'])]):
                        self._con.send(id=dest_id,
                                       msg=dict(type='reconfig',
                                                node_list=[self.id],
                                                frag_id=self.id))
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
                        default=11,
                        type=int)
    parser.add_argument('--wait',
                        default=11,
                        type=int,
                        help='Time to wait before we start to send heartbit')
    parser.add_argument('--net_config', default='config/sample_graph.json', type=str)
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

    receiver.join()
    sender.join()
    node.join()