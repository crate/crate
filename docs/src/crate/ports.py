# -*- coding: utf-8 -*-

import socket

def public_ip():
    """
    take first public interface
    sorted by getaddrinfo - see RFC 3484
    should have the real public ip as first
    """
    for addrinfo in socket.getaddrinfo(socket.gethostname(), None):
        if addrinfo[1] in (socket.SOCK_STREAM, socket.SOCK_DGRAM):
            return addrinfo[4][0]


def random_available_port():
    """return some available port reported by the kernel"""
    sock = socket.socket()
    sock.bind(('', 0))
    port = sock.getsockname()[1]
    sock.close()
    return port