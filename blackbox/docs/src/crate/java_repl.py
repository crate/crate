import subprocess
import json
import os
import time
import socket
from urllib.request import urlopen
from urllib.parse import urlencode


def is_up(host, port):
    """test if a host is up"""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    ex = s.connect_ex((host, int(port)))
    if ex == 0:
        s.close()
        return True
    return False


class JavaRepl(object):

    __bases__ = ()
    __name__ = 'javarepl'

    def __init__(self, java_repl_jar, jars, port):
        self.port = port = str(port)
        jars = [os.path.abspath(p) for p in [java_repl_jar] + jars]
        cp = ':'.join(jars)
        self.cmd = [
            'java', '-cp', cp, 'javarepl.Repl', '--port=' + port]
        self.url = 'http://localhost:' + port + '/execute'

    def setUp(self):
        self.process = subprocess.Popen(
            self.cmd,
            stderr=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stdin=subprocess.PIPE)
        slept = 0
        while not is_up('localhost', self.port) and slept < 10:
            time.sleep(0.1)
            slept += 0.1

    def tearDown(self, *args, **kwargs):
        self.process.stdin.close()
        self.process.stdout.close()
        self.process.stderr.close()
        self.process.kill()
        self.process.wait()

    def execute(self, expression):
        data = urlencode({'expression': expression}).encode('utf-8')
        r = urlopen(self.url, data=data)
        r = json.loads(r.read().decode('utf-8'))
        for log in r['logs']:
            if log['type'] == 'ERROR' and not log['message'].startswith('log4j'):
                return log['message']
