import json
import socket

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


if __name__ == '__main__':
    config = load_config('simple_graph.json')
    start_msg = _encode(dict(type="start"))
    for node in config:
        send_msg(start_msg,
                 host=start_msg['host'],
                 port=start_msg['port'])
        print(node)