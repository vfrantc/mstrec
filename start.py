#!/usr/bin/env python3
import json
import socket
import argparse
from pprint import pprint

def _encode(msg):
    '''Converts dictionary to binary coded json-string which can be send over the network'''
    json_str = json.dumps(msg)
    return bytes(json_str, encoding='utf-8')


def load_config(fname):
    with open(fname, 'r') as fd:
        return json.load(fd)


def send_msg(msg, host, port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect((host, port))
        sock.sendall(msg)
    except ConnectionRefusedError:
        pass
    finally:
        sock.close()


def opt_parser():
    parser = argparse.ArgumentParser(description='Network reconfiguration node')
    parser.add_argument('--net_config', default='sample_graph.json', type=str)
    return parser


if __name__ == '__main__':
    parser = opt_parser()
    opt = parser.parse_args()

    config = load_config(opt.net_config)
    pprint(config)

    start_msg = _encode(dict(type="start"))
    for node in config.values():
        send_msg(start_msg,
                 host=node['host'],
                 port=node['port'])
        print(node)