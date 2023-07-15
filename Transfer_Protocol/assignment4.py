import ipaddress
import pathlib
from ipaddress import IPv4Address, IPv6Address
from typing import Union

import click as click

import socket
import re
import sys


BUFFER_SIZE = 1024
TIME_OUT = 0.5


def string_to_int(string: str) -> int:
    # "+5" and "05" are considered invalid
    pattern = r'^[1-9]\d*$'
    if not re.match(pattern, string):
        return None
    return int(string)


def string_to_netstring(string: str) -> str:
    length = len(string.encode())
    return f'{length}:{string},'


# def bytes_to_netstring(bytes: bytes) -> bytes:
#     length = len(bytes)
#     return f'{length}:'.encode() + bytes + b','


# def netstring_to_string(netstring: bytes) -> bytes:
#     pattern = r'^([1-9]\d*):(.+),$'
#     match = re.match(pattern, netstring)
#     if match is None:
#         return None
#     length = string_to_int(match.group(1))
#     string = match.group(2)
#     if len(string.encode()) != length:
#         return None
#     return string
    
    
def send_error(error: str, sock: socket.socket):
    sock.sendall(string_to_netstring(f'E {error}').encode())
    print(f'Error: {error}')
    sock.close()
    sys.exit()
    
    
def send_message(message: str, sock: socket.socket):
    sys.stderr.write('Sent: {}\n'.format(message))
    sock.sendall(string_to_netstring(message).encode())
    
    
def receive_message_queue():
    # stream-oriented
    # closure
    control_netstrings = b''
    data_netstrings = b''
    
    # channel: 0 for control channel, 1 for data channel
    def inner(sock: socket.socket, channel: int) -> bytes:
        nonlocal control_netstrings, data_netstrings
        netstrings = control_netstrings if channel == 0 else data_netstrings
        while True:
            try:
                data = sock.recv(BUFFER_SIZE)
                sys.stderr.write(f'Received_segment: {data}\n')
                if not data:
                    break
                netstrings += data
            except socket.timeout:
                break
        
        sys.stderr.write(f'Netstrings: {netstrings}\n')
        if netstrings == b'':
            error = 'no netstring received'
            sock.sendall(string_to_netstring(f'E {error}').encode())
            print(f'Error: {error}')
            sock.close()
            sys.exit(1)
        try:
            length, data = netstrings.split(b':', 1)
        except ValueError:
            send_error(f'invalid netstring: {netstrings}, no colon', sock)
            
        if length.startswith(b'0') or length.startswith(b'+'):
            send_error(f'invalid netstring: {netstrings}, startswith 0 or +', sock)
        try:
            length = int(length)
        except ValueError:
            send_error(f'invalid netstring: {netstrings}, invalid length', sock)
        
        if len(data) < length + 1 or data[length:length + 1] != b',':
            send_error(f'invalid netstring: {netstrings}, no comma', sock)
            
        message = data[:length]
        netstrings = data[length + 1:] if len(data) > length + 1 else b''
        if channel == 0:
            control_netstrings = netstrings
        else:
            data_netstrings = netstrings
        
        sys.stderr.write(f'Received: {message}\n')
        if message is None:
            send_error(f'invalid netstring: {message}', sock)
        if message.split(b' ')[0] == b'E':
            send_error(message.split(b' ')[1].decode(), sock)
        return message
    
    return inner

receive_message = receive_message_queue()


def build_data_channel(message: str, nick: str, dst_address: IPv6Address, data_channel: socket.socket, token):
    # Build the data channel with the server
    data_channel.listen(1)
    sys.stderr.write('Listening on port {}\n'.format(data_channel.getsockname()[1]))
    server_socket, server_address = data_channel.accept()
    sys.stderr.write('Connected to {}\n'.format(server_address))
    if server_address[0] != str(dst_address):
        send_error('invalid server address: {}, expected: {}'.format(server_address[0], str(dst_address)), server_socket)
    server_socket.settimeout(TIME_OUT)
    
    data = receive_message(server_socket, 1)
    if data != b'T GRNVS V:1.0':
        send_error(f'invalid message: {data}, expected T GRNVS V:1.0', server_socket)
    
    response = f'D {nick}'
    send_message(response, server_socket)
    
    data = receive_message(server_socket, 1)
    try:
        begin, received_token = data.split(b' ')
        if begin == b'E':
            send_error(received_token, server_socket)
        elif begin != b'T' or received_token != token:
            raise ValueError
    except:
        send_error(f'invalid message: {data}, expected T {token}', server_socket)
    
    response = f'D {message}'
    send_message(response, server_socket)
    
    data = receive_message(server_socket, 1)
    server_socket.close()
    return data


def run(port: int, message: str, nick: str, dst_address: IPv6Address):
    # With Python, you can use almost the same socket interface as in C
    # Have a look at https://docs.python.org/3/library/socket.html

    # Build the control channel
    with socket.socket(socket.AF_INET6, socket.SOCK_STREAM) as control_channel:
        control_channel.connect((str(dst_address), port))
        control_channel.settimeout(TIME_OUT)
        
        init_sequence = 'C GRNVS V:1.0'
        send_message(init_sequence, control_channel)
        
        response = receive_message(control_channel, 0)
        if response != b'S GRNVS V:1.0':
            send_error(f'invalid message: {response}, expected S GRNVS V:1.0', control_channel)
        
        send_message(f'C {nick}', control_channel)
        
        response = receive_message(control_channel, 0)
        try:
            begin, token = response.split(b' ', 1)  # split only once
            if begin == b'E':
                send_error(token, control_channel)
            elif begin != b'S':
                raise ValueError
        except:
            send_error(f'invalid message: {response}, expected S <token>', control_channel)
        
        # Open an available port
        with socket.socket(socket.AF_INET6, socket.SOCK_STREAM) as data_channel:
            data_channel.bind(('', 0))
            # print(data_channel.getsockname())
            dport = data_channel.getsockname()[1]
            
            send_message(f'C {dport}', control_channel)
            
            # Build the data channel
            dtoken_data = build_data_channel(message, nick, dst_address, data_channel, token)
            try:
                # token may contain '\n', ' ' and non-utf8 characters
                begin, dtoken = dtoken_data.split(b' ', 1)  # split only once
                if begin == b'E':
                    send_error(dtoken, control_channel)
                elif begin != b'T':
                    raise ValueError
            except:
                send_error(f'invalid message: {dtoken_data}, expected T <token>', control_channel)
        
        try:  # utf-8
            data = receive_message(control_channel, 0)
            data = data.decode()
        except UnicodeDecodeError:
            send_error(f'invalid utf-8 message: {data}', control_channel)
        try:
            begin, msglen = data.split(' ')
            if begin != 'S':
                raise ValueError
            msglen = string_to_int(msglen)
            if msglen is None:
                raise ValueError
        except:
            send_error(f'invalid message: {data}, expected S <msglen>', control_channel)
        
        if msglen != len(message.encode()):
            send_error('wrong message length: {}, expected: {}'.format(msglen, len(message.encode())), control_channel)
        
        sent_bytes = 'C '.encode() + dtoken
        sys.stderr.write(f'Sent: {sent_bytes}\n')
        control_channel.sendall(str(len(sent_bytes)).encode() + b':' + sent_bytes + b',')
        
        data = receive_message(control_channel, 0)
        if data != b'S ACK':
            send_error(f'invalid message: {data}, expected S ACK', control_channel)
    

@click.command()
@click.option('-p', '--port', type=click.IntRange(min=1, max=2 ** 16, max_open=True), default=1337, help='the port the client should connect to')
@click.option('-m', '--message', type=str, help='the message to send, spaces might need to be quoted in the shell')
@click.option('-f', '--file', type=click.Path(exists=True, dir_okay=False, path_type=pathlib.Path), help='a file to use as message, -m will be ignored')
@click.argument('nick', type=str, required=True)  # the nick that should be displayed on the server
@click.argument('destination', type=str, required=True)  # the destination IPv6 address
def main(port: int, message: str, file: pathlib.Path, nick: str, destination: str):
    if not file and not message:
        raise SystemExit("-m <message> or -f <file> must be given!")
    if file:
        message = file.read_text()
    dst_address = ipaddress.IPv6Address(destination)
    run(port, message, nick, dst_address)


if __name__ == '__main__':
    main()
