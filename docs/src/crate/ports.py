# -*- coding: utf-8 -*-

import socket

def random_available_port():
    """return some available port reported by the kernel"""
    sock = socket.socket()
    sock.bind(('', 0))
    port = sock.getsockname()[1]
    sock.close()
    return port