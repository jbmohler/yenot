import os
import sys
import subprocess
import contextlib
import requests


class RunningServer:
    def __init__(self, url):
        self.url = url


@contextlib.contextmanager
def server_running(dburl, modules=None, sitevars=None):
    if modules == None:
        modules = []
    modules.insert(0, "yenot.server.tests")
    if sitevars == None:
        sitevars = []

    repo_root = "."
    if "YENOT_REPO" in os.environ:
        repo_root = os.environ["YENOT_REPO"]
    args = (
        [sys.executable, os.path.join(repo_root, "scripts/yenotserve.py")]
        + [f"--module={m}" for m in modules]
        + [f"--sitevar={m}" for m in sitevars]
        + [dburl]
    )
    p = subprocess.Popen(args, stdin=sys.stdin, stdout=sys.stdout, stderr=sys.stderr)

    port = 8080
    if "YENOT_PORT" in os.environ:
        port = int(os.environ["YENOT_PORT"])
    url = f"http://127.0.0.1:{port}"

    while True:
        try:
            r = requests.get(f"{url}/api/ping")
            if r.status_code == 200:
                break
        except:
            pass

        try:
            p.wait(0.2)
            if p.returncode != None:
                break
        except subprocess.TimeoutExpired:
            pass

    if p.returncode == None:
        print("server up & running")

        # do cool testing things
        try:
            yield RunningServer(url)
        finally:
            try:
                requests.put(f"{url}/api/server/shutdown")
                # p.terminate()

                print("waiting on process to close")
                p.wait()
                print("closed; exiting")
            finally:
                p.terminate()
    else:
        print("server failed to start")
