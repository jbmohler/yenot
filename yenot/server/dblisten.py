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
    def __init__(self, key, channel):
        self.key = key
        self.channel = channel

        if re.match("[a-zA-Z_][a-zA-Z0-9_]*", self.channel) is None:
            raise RuntimeError(
                "the listen channel must be a valid python identifier to protect against sql injection"
            )

        self.event = threading.Event()
        self.thislist = []

        self.last_check = time.time()

        self.change_queue_core()

    @staticmethod
    def start_change_queue(key, channel):
        global LISTENERS

        if key in LISTENERS:
            return LISTENERS[key]
        else:
            new = Listener(key, channel)
            LISTENERS[key] = new
            return new

    def change_queue_core(self):
        index = 0
        with app.dbconn() as conn:
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

            curs = conn.cursor()
            # The channel string shall not be quoted in this context.
            curs.execute("LISTEN {};".format(self.channel))

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

                cutoff = time.time() - 60
                for cut, thing in enumerate(self.thislist):
                    if thing[0] > cutoff:
                        self.thislist = self.thislist[cut:]
                        break

    def changes_since(self, index):
        changes = rtlib.simple_table(["index", "payload"])

        wait_count = 4
        wait_length = 10

        for i in range(wait_count):
            for thing in self.thislist:
                if thing[1] > index:
                    with changes.adding_row() as r2:
                        r2.index = thing[1]
                        r2.payload = thing[2]
                    index = thing[1]

            if i < wait_count - 1 and len(changes.rows) == 0:
                self.event.wait(wait_length)

        self.last_check = time.time()
        return changes


@app.get("/api/sql/changequeue", name="api_sql_changequeue")
def api_sql_changequeue():
    try:
        key = request.query.get("key")
        channel = request.query.get("channel")
        index = request.query.get("index", None)

        index = 0 if index is None else int(index)
        listener = Listener.start_change_queue(key, channel)

        # return anything since
        results = api.Results()
        results.tables["changes", True] = listener.changes_since(index).as_tab2()
    except Exception as e:
        import traceback

        print(traceback.print_exc())
    return results.json_out()
