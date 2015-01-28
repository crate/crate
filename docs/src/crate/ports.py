# -*- coding: utf-8 -*-

import socket

def public_ipv4():
    """
    take first public interface
    sorted by getaddrinfo - see RFC 3484
    should have the real public IPv4 address as first address.
    At the moment the test layer is not able to handle v6 addresses
    """
    for addrinfo in socket.getaddrinfo(socket.gethostname(), None):
        if addrinfo[1] in (socket.SOCK_STREAM, socket.SOCK_DGRAM) and addrinfo[0] == socket.AF_INET:
            return addrinfo[4][0]


def random_available_port():
    """return some available port reported by the kernel"""
    sock = socket.socket()
    sock.bind(('', 0))
    port = sock.getsockname()[1]
    sock.close()
    return port