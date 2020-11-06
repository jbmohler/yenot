import re
import time
import threading
import select
import psycopg2
import psycopg2.extensions
import yenot.backend.api as api
import rtlib
from bottle import request


app = api.get_global_app()

LISTENERS = {}


class Listener:
    def __init__(self, channel):
        self.channel = channel

        if re.match("[a-zA-Z_][a-zA-Z0-9_]*", self.channel) is None:
            raise RuntimeError(
                "the listen channel must be a valid python identifier to protect against sql injection"
            )

        self.event = threading.Event()
        self.thislist = []

        self.last_check = time.time()

        self.qthread = threading.Thread(target=self.change_queue_core)
        self.qthread.start()

    @staticmethod
    def start_change_queue(key, channel):
        # TODO: remove key
        global LISTENERS

        if channel in LISTENERS:
            return LISTENERS[channel]
        else:
            new = Listener(channel)
            LISTENERS[channel] = new
            return new

    @staticmethod
    def stop_change_queue(channel):
        global LISTENERS
        del LISTENERS[channel]

    def change_queue_core(self):
        index = 0
        with app.background_dbconn() as conn:
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

            curs = conn.cursor()
            # The channel string shall not be quoted in this context.
            curs.execute(f"LISTEN {self.channel};")

            while time.time() - self.last_check < 30:
                if select.select([conn], [], [], 5) == ([], [], []):
                    pass  # print(f"nothing; iterate {self.channel}")
                else:
                    # print(f"poll it {self.channel}")
                    conn.poll()
                    while conn.notifies:
                        notify = conn.notifies.pop(0)

                        index += 1
                        node = (time.time(), index, notify.payload)
                        self.thislist.append(node)
                        self.event.set()
                        self.event.clear()

                cutoff = time.time() - 60
                for cut, chrow in enumerate(self.thislist):
                    if chrow[0] > cutoff:
                        self.thislist = self.thislist[cut:]
                        break

        self.stop_change_queue(self.channel)

    def current_index(self):
        if len(self.thislist) > 0:
            return self.thislist[-1][1]
        else:
            return 0

    def changes_since(self, cancel, index):
        changes = rtlib.simple_table(["index", "payload"])

        wait_count = 10
        wait_length = 4

        for i in range(wait_count):
            self.last_check = time.time()

            for chrow in self.thislist:
                if chrow[1] > index:
                    with changes.adding_row() as r2:
                        r2.index = chrow[1]
                        r2.payload = chrow[2]
                    index = chrow[1]

            if len(changes.rows) > 0:
                break

            if i < wait_count - 1:
                self.event.wait(wait_length)
                if not self.event.is_set():
                    # give a chance for the cancel exception
                    cancel.wait(0.01)

        return changes


@app.put("/api/sql/changequeue", name="put_api_sql_changequeue")
def put_api_sql_changequeue():
    key = request.query.get("key")
    channel = request.query.get("channel")

    listener = Listener.start_change_queue(key, channel)

    # return anything since
    results = api.Results()
    results.keys["index"] = listener.current_index()
    return results.json_out()


@app.get("/api/sql/changequeue", name="get_api_sql_changequeue")
def get_api_sql_changequeue():
    key = request.query.get("key")
    channel = request.query.get("channel")
    index = request.query.get("index", None)

    index = 0 if index is None else int(index)
    listener = Listener.start_change_queue(key, channel)

    results = api.Results()
    with app.cancel_queue() as cancel:
        # return anything since
        results.tables["changes", True] = listener.changes_since(
            cancel, index
        ).as_tab2()
    return results.json_out()
