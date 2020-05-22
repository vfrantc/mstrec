#!/usr/bin/env python3
import argparse
from pprint import pprint

import socket
import threading
import networkx as nx
import matplotlib.pyplot as plt

from common import load_config
from common import encode
from common import send_msg

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
    return parser


if __name__ == '__main__':
    parser = opt_parser()
    opt = parser.parse_args()

    config = load_config(opt.net_config)
    pprint(config)

    start_msg = encode(dict(type="fetch"))
    for node in config.values():
        send_msg(start_msg,
                 host=node['host'],
                 port=node['port'])
        print(node)

    graph = as_nx(id_table, edges=True)
    nx.draw(graph, with_labels=True)
    plt.draw()
    plt.show()
