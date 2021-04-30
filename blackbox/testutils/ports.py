# Licensed to Crate.io GmbH ("Crate") under one or more contributor
# license agreements.  See the NOTICE file distributed with this work for
# additional information regarding copyright ownership.  Crate licenses
# this file to you under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.  You may
# obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
# License for the specific language governing permissions and limitations
# under the License.
#
# However, if you have executed another commercial license agreement
# with Crate these terms will supersede the license and you may use the
# software solely pursuant to the terms of the relevant commercial agreement.


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


def bind_port(addr='127.0.0.1', port=0):
    with socket.socket() as sock:
        sock.bind((addr, port))
        port = sock.getsockname()[1]
        try:
            sock.shutdown(socket.SHUT_RDWR)
        except Exception:
            # ok, at least we know that the socket is not connected
            pass
        return port


def _bind_range(addr, size):
    start = bind_port(addr)
    yield start
    end = start + size + 1
    for i in range(start + 1, end):
        yield bind_port(addr, i)


def bind_range(addr='127.0.0.1', range_size=1):
    max_retries = 10
    for _ in range(max_retries):
        try:
            ports = list(_bind_range(addr, range_size))
            return f'{ports[0]}-{ports[-1]}'
        except Exception:
            continue
    raise OSError("Could not get free port range. Max retries exceeded.")
