#!/usr/bin/env python3

import socket
import os
import signal
import subprocess
from subprocess import (check_output, run)


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

    output = check_output(['jps'], universal_newlines=True)
    for line in output.split('\n'):
        try:
            pid, procname = line.split(' ')
        except ValueError:
            continue
        if procname == 'CrateDB':
            print(f'CrateDB process with pid {pid} found. Killing it')
            os.kill(int(pid), signal.SIGKILL)

    try:
        containers = check_output(['docker ps -q'], universal_newlines=True)
    except subprocess.CalledProcessError:
        return
    for container in containers.split('\n'):
        run(['docker', 'stop', container])
        run(['docker', 'rm', container])


if __name__ == "__main__":
    kill()
