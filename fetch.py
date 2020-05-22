#!/usr/bin/env python3
import argparse
from pprint import pprint

import socket
import threading
from queue import Queue
import networkx as nx
import matplotlib.pyplot as plt
import time
from common import load_config
from common import encode
from common import decode
from common import send_msg

class Server(threading.Thread):

    def __init__(self, host, port, size=5):
        super().__init__()
        self._host = host
        self._port = port
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind((self._host, self._port))
        self.size = size
        self.responses = Queue()

    def run(self):
        self._sock.listen(5)
        while True:
            # if got all the responses
            # TODO: This is a problem
            if self.responses.qsize() == self.size:
                break
            client, addr = self._sock.accept()
            client.settimeout(5)
            threading.Thread(target=self.listen_to_client, args=(client, addr)).start()

    def listen_to_client(self, client, addr):
        size = 1024
        while True:
            try:
                data = client.recv(size)
                if data:
                    self.responses.put(decode(data))
            except:
                client.close()
                return False



def as_nx(id_table, edges=False):
    '''Convert to networkx graph.
       By default saves links'''
    graph = nx.Graph()
    graph.add_nodes_from(id_table.keys())
    for idx, node in id_table.items():
        if edges:
            for edge_id in node.edges:
                print('add edge: {} - {}'.format(node.id, node.ports[edge_id]))
                graph.add_edge(node.id, node.ports[edge_id])
        else:
            for neighbour in node.ports.values():
                graph.add_edge(node.id, neighbour)
    return graph


def opt_parser():
    parser = argparse.ArgumentParser(description='Network reconfiguration node')
    parser.add_argument('--net_config', default='config/sample_graph2.json', type=str)
    parser.add_argument('--host', default='127.0.0.1', type=str)
    parser.add_argument('--port', default=3333, type=int)
    return parser


if __name__ == '__main__':
    parser = opt_parser()
    opt = parser.parse_args()

    config = load_config(opt.net_config)
    #pprint(config)

    # Start server here
    server = Server(host=opt.host,
                    port=opt.port,
                    size=len(config))
    server.start()
    time.sleep(0.1)

    fetch_msg = encode(dict(type="extract", host=opt.host, port=opt.port))
    for node in config.values():
        send_msg(fetch_msg,
                 host=node['host'],
                 port=node['port'])
        #print(node)
    time.sleep(0.1)

    # wait for server here
    server.join()

    while not server.responses.empty():
        resp = server.responses.get()
        print(repr(resp))

    # server.responses.get()
    # graph = as_nx(id_table, edges=True)
    # nx.draw(graph, with_labels=True)
    # plt.draw()
    # plt.show()
