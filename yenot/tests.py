import os
import sys
import subprocess
import traceback
import contextlib
import requests

class RunningServer:
    def __init__(self, url):
        self.url = url

@contextlib.contextmanager
def server_running(dburl, modules=None):
    if modules == None:
        modules = []
    modules.insert(0, 'yenot.server.tests')

    args = [sys.executable, '../yenot/scripts/yenotserve.py'] + \
                ['--module={}'.format(m) for m in modules] + \
                [dburl]
    p = subprocess.Popen(args, stdin=sys.stdin, stdout=sys.stdout, stderr=sys.stderr)

    url = 'http://127.0.0.1:8080'

    while True:
        try:
            r = requests.get('{}/api/ping'.format(url))
            if r.status_code == 200:
                break
        except:
            pass

        try:
            p.wait(.2)
            if p.returncode != None:
                break
        except subprocess.TimeoutExpired:
            pass

    if p.returncode == None:
        print('server up & running')

        # do cool testing things
        try:
            yield RunningServer(url)
        except:
            print('*** Test runner traceback ***')
            traceback.print_exc()

        requests.put('{}/api/server/shutdown'.format(url))
        #p.terminate()

        print('waiting on process to close')
        p.wait()
        print('closed; exiting')
    else:
        print('server failed to start')

 
