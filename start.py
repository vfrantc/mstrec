#!/usr/bin/env python3
import argparse
from pprint import pprint

from common import load_config
from common import encode
from common import send_msg


def opt_parser():
    parser = argparse.ArgumentParser(description='Network reconfiguration node')
    parser.add_argument('--net_config', default='config/sample_graph2.json', type=str)
    return parser


if __name__ == '__main__':
    parser = opt_parser()
    opt = parser.parse_args()

    config = load_config(opt.net_config)
    pprint(config)

    start_msg = encode(dict(type="start"))
    for node in config.values():
        send_msg(start_msg,
                 host=node['host'],
                 port=node['port'])
        print(node)