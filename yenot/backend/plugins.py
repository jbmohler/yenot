import os
import sys
import re
import json
import contextlib
import urllib.parse
import traceback
import time
import threading
import psycopg2
import psycopg2.extensions
import psycopg2.extras
import psycopg2.pool
import bottle
from bottle import request, response
import rtlib
from . import misc

# to become a method of app
@contextlib.contextmanager
def dbconn(self):
    conn = self.pool.getconn()
    ctoken = getattr(request, 'cancel_token', None)
    try:
        if ctoken != None:
            self.register_connection(ctoken, conn)
        yield conn
    finally:
        if ctoken != None:
            self.unregister_connection(ctoken, conn)
        self.pool.putconn(conn)

def register_connection(self, ctoken, conn):
    if ctoken in self.dbconn_register:
        self.dbconn_register[ctoken].append(conn)
    else:
        self.dbconn_register[ctoken] = [conn]

def unregister_connection(self, ctoken, conn):
    if ctoken in self.dbconn_register:
        self.dbconn_register[ctoken].remove(conn)
        if len(self.dbconn_register[ctoken]) == 0:
            del self.dbconn_register[ctoken]
 
def cancel_request(self, cancel_token):
    if cancel_token in self.dbconn_register:
        connections = self.dbconn_register[cancel_token]
        for conn in connections:
            conn.cancel()
    else:
        raise misc.UserError('invalid-param', 'This is not a recognized request or not capable of being canceled.')

def create_connection(dburl):
    result = urllib.parse.urlsplit(dburl)

    dbname = result.path[1:]

    kwargs = {'dbname': result.path[1:]}
    if result.hostname != None:
        kwargs['host'] = result.hostname
    if result.port != None:
        kwargs['port'] = result.port
    if result.username != None:
        kwargs['user'] = result.username
    if result.password != None:
        kwargs['password'] = result.password
    kwargs['cursor_factory'] = psycopg2.extras.NamedTupleCursor

    return psycopg2.connect(**kwargs)

def create_pool(dburl):
    result = urllib.parse.urlsplit(dburl)

    dbname = result.path[1:]

    kwargs = {'dbname': result.path[1:]}
    if result.hostname not in [None, '']:
        kwargs['host'] = result.hostname
    if result.port != None:
        kwargs['port'] = result.port
    if result.username != None:
        kwargs['user'] = result.username
    if result.password != None:
        kwargs['password'] = result.password
    kwargs['cursor_factory'] = psycopg2.extras.NamedTupleCursor

    return psycopg2.pool.ThreadedConnectionPool(4, 16, **kwargs)

def delayed_shutdown(self):
    def make_it_stop():
        time.sleep(.3)
        self.pool.closeall()
        self._paste_server.stop()
    self.stop_thread = threading.Thread(target=make_it_stop)
    self.stop_thread.start()

global_app = None

class DerivedBottle(bottle.Bottle):
    pass

# resume imports
from paste import httpserver
from paste.translogger import TransLogger

# See http://stackoverflow.com/questions/32404/

class PasteServer(bottle.ServerAdapter):
    def run(self, handler):
        # Send parameter start_loop=false so we can put the paste server in a
        # variable for later stopping.
        handler = TransLogger(handler, setup_console_handler=(not self.quiet))
        self.paste = httpserver.serve(handler, host=self.host, port=str(self.port), start_loop=False)
        self.paste.serve_forever()

    def stop(self):
        self.paste.server_close()


def init_application(dburl):
    global global_app

    DerivedBottle.dbconn = dbconn
    DerivedBottle.register_connection = register_connection
    DerivedBottle.unregister_connection = unregister_connection
    DerivedBottle.cancel_request = cancel_request
    DerivedBottle.delayed_shutdown = delayed_shutdown

    app = DerivedBottle()
    global_app = app

    app.pool = create_pool(dburl)
    app.dbconn_register = {}

    app.install(RequestCancelTracker())
    app.install(ExceptionTrapper())

    # hook up the basic stuff
    import yenot.server
    port = 8080
    if 'YENOT_PORT' in os.environ:
        port = int(os.environ['YENOT_PORT'])
    app._paste_server = PasteServer(host='127.0.0.1', port=port)

    return app

class RequestCancelTracker:
    name = 'yenot-cancel'
    api = 2

    def setup(self, app):
        self.app = app

    def apply(self, callback, route):
        def wrapper(*args, **kwargs):
            request.cancel_token = request.headers.get('X-Yenot-CancelToken', None)
            return callback(*args, **kwargs)
        return wrapper

class ExceptionTrapper:
    name = 'yenot-exceptions'
    api = 2

    def setup(self, app):
        # expect app to have dbconn
        self.app = app

    def report(self, e, myresponse, keys):
        with self.app.dbconn() as conn:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            fsumm = [(f.filename, f.lineno, f.name) for f in traceback.extract_tb(exc_traceback, 15)]
            details = { \
                    'exc_type': exc_type.__name__,
                    'exception': str(exc_value),
                    'session': request.headers.get('X-Yenot-SessionID', None),
                    'frames': list(reversed(fsumm))}
            des = 'HTTP {} - {}'.format(myresponse.status, keys.get('error-msg', None))
            misc.write_event_entry(conn, 'Yenot Server Error', des, details)
            conn.commit()

    def apply(self, callback, route):
        def wrapper(*args, **kwargs):
            try:
                return callback(*args, **kwargs)
            except psycopg2.IntegrityError as e:
                if str(e).startswith('duplicate key value violates unique constraint'):
                    match = re.search(r'.*Key \([a-zA-Z0-9_]*\)=\((.*)\) already exists.', str(e))
                    msg = 'A duplicate key was found.'
                    if match != None:
                        msg = 'A duplicate key with value "{}" was found.'.format(match.group(1))
                    keys = {'error-key': 'duplicate-key', 'error-msg': msg}
                elif str(e).startswith('null value in column '):
                    match = re.match(r'null value in column "([a-zA-Z0-9_]*)" violates not-null constraint', str(e).split('\n')[0])
                    msg = 'The value in field "{}" must be non-empty and valid.'.format(match.group(1))
                    keys = {'error-key': 'null-value', 'error-msg': msg}
                else:
                    keys = {'error-key': 'data-integrity', 'error-msg': 'An invalid value was passed to the database.\n\n{}'.format(str(e))}
                response.status = 403
                self.report(e, response, keys)
                return json.dumps([keys])
            except psycopg2.ProgrammingError as e:
                if bottle.DEBUG:
                    traceback.print_exc()
                    sys.stderr.flush()
                try:
                    prim = e.diag.message_primary
                    lines = e.pgerror.split('\n')
                    if len(lines) >= 3 and lines[1].startswith('LINE') and lines[2].find('^') >= 0:
                        c2 = lines[2].find('^')
                        s2 = lines[1]
                        sec = s2[:c2]+'##'+s2[c2:]
                    elif len(lines) >= 2:
                        sec = lines[1]
                    else:
                        sec = None
                    if sec == None:
                        errdesc = prim
                    else:
                        errdesc = '{} ({})'.format(prim, sec)
                except Exception:
                    errdesc = str(e)
                keys = {'error-key': 'sql-syntax-error', 'error-msg': 'SQL Error:  {}'.format(errdesc)}
                response.status = 500
                response.content_type = 'application/json; charset=UTF-8'
                self.report(e, response, keys)
                return json.dumps([keys])
            except psycopg2.extensions.QueryCanceledError:
                keys = {'error-key': 'cancel', 'error-msg': 'Client cancelled request'}
                response.status = 403
                return json.dumps([keys])
            except misc.UserError as e:
                keys = {'error-key': e.key, 'error-msg': str(e)}
                response.status = 403
                response.content_type = 'application/json; charset=UTF-8'
                return json.dumps([keys])
            except bottle.HTTPError:
                raise
            except Exception as e:
                if bottle.DEBUG:
                    traceback.print_exc()
                    sys.stderr.flush()

                keys = {'error-msg': str(e)}
                response.status = 500
                response.content_type = 'application/json; charset=UTF-8'
                self.report(e, response, keys)
                return json.dumps([keys])
        return wrapper
