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
def server_running(dburl, modules=None, sitevars=None):
    if modules == None:
        modules = []
    modules.insert(0, 'yenot.server.tests')
    if sitevars == None:
        sitevars = []

    repo_root = '.'
    if 'YENOT_REPO' in os.environ:
        repo_root = os.environ['YENOT_REPO']
    args = [sys.executable, os.path.join(repo_root, 'scripts/yenotserve.py')] + \
                ['--module={}'.format(m) for m in modules] + \
                ['--sitevar={}'.format(m) for m in sitevars] + \
                [dburl]
    p = subprocess.Popen(args, stdin=sys.stdin, stdout=sys.stdout, stderr=sys.stderr)

    port = 8080
    if 'YENOT_PORT' in os.environ:
        port = int(os.environ['YENOT_PORT'])
    url = 'http://127.0.0.1:{}'.format(port)

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

 
