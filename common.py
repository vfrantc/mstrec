import json
import socket


def encode(msg):
    '''Converts dictionary to binary coded json-string which can be send over the network'''
    json_str = json.dumps(msg)
    return bytes(json_str, encoding='utf-8')


def decode(msg):
    '''Decodes binary data to dictionary'''
    result = None
    try:
        result = json.loads(msg.decode("utf-8"))
    except Exception:
        pass
    return result


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