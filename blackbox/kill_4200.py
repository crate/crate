#!/usr/bin/env python3

import socket
import os
import signal
import subprocess


def is_up(host: str, port: int) -> bool:
    try:
        conn = socket.create_connection((host, port))
        conn.close()
        return True
    except (socket.gaierror, ConnectionRefusedError):
        return False


def kill():
    if not is_up('localhost', 4200):
        return

    output = subprocess.check_output(['jps'], universal_newlines=True)
    for line in output.split('\n'):
        try:
            pid, procname = line.split(' ')
        except ValueError:
            continue
        if procname == 'CrateDB':
            print(f'CrateDB process with pid {pid} found. Killing it')
            os.kill(int(pid), signal.SIGKILL)


if __name__ == "__main__":
    kill()
