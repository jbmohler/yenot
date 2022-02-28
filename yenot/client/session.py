import json
import uuid
import functools
import requests
import rtlib


class YenotError(Exception):
    pass


class YenotServerError(YenotError):
    pass


class YenotSession(requests.Session):
    def __init__(self, server_url):
        super(YenotSession, self).__init__()
        self.server_url = server_url
        if not self.server_url.endswith("/"):
            self.server_url += "/"
        self.mount(self.server_url, requests.adapters.HTTPAdapter(max_retries=3))

    def prefix(self, tail):
        return self.server_url + tail

    def raw_client(self):
        return YenotClient(self, lambda x: x)

    def std_client(self):
        return YenotClient(self, StdPayload)

    def json_client(self):
        return YenotClient(self, json.loads)


def exception_string(response, method):
    if response.status_code == 401:
        return f"The request to {response.url} is not authorized."
    return """\
Server request failed with status code:  {0.status_code}
URL:  {0.url}
Method:  {1}""".format(
        response, method
    )


def raise_exception_ex(response, method):
    if response.status_code in (400, 403):
        t = response.text
        is_json = len(t) > 0 and t[0] == "[" and t[-1] == "]"
        if is_json:
            keys = json.loads(response.text)[0]
            if "error-msg" in keys:
                exc = YenotError(keys["error-msg"])
                exc.status_code = response.status_code
                raise exc
    exc = YenotServerError(exception_string(response, method))
    exc.status_code = response.status_code
    raise exc


class StdPayload:
    def __init__(self, rawpay):
        self._pay = json.loads(rawpay)

    @property
    def keys(self):
        return self._pay

    def named_table(self, name, mixin=None):
        return rtlib.ClientTable(*self._pay[name], mixin=mixin)

    def main_table(self, mixin=None):
        mn = self._pay["__main_table__"]
        return self.named_table(mn, mixin)


class RequestFuture:
    def __init__(self, session, executor):
        self.session = session
        self.executor = executor
        self.cancel_token = str(uuid.uuid1())
        self.cancelled = False
        self.running = False

    def cancel(self):
        self.cancelled = True
        if self.running and self.cancel_token != None:
            self.session.put(
                self.session.prefix("api/request/cancel"),
                params={"token": self.cancel_token},
            )

    def result(self):
        return self._future.result()

    def internal_get(self, client, *args, **kwargs):
        kwargs["cancel_token"] = self.cancel_token
        try:
            self.running = True
            result = client.get(*args, **kwargs)
        finally:
            self.running = False
            self.cancel_token = None
        return result

    def outer_get(self, client, *args, **kwargs):
        self._future = self.executor.submit(self.internal_get, client, *args, **kwargs)
        return self


class YenotClient:
    """
    This class implements the following client-side functionality of the Yenot
    server:

    - Unpacking the body of the response (currently json.loads)

    It does so with-out any GUI toolkit dependency.  Errors and progress
    indications are implemented via exceptions and callbacks.  Background
    threads will be used and managed internally as appropriate (although
    "appropriate" has not been clearly defined nor implemented).

    The star of this class is get which sends a REST request to the specified
    Yenot server via the Python requests library.  It notifies the user of errors
    by message box or exception as appropriate and configured by a callback
    (?).  If no error occurs the response is parsed by json.loads and returned
    with-out further parsing.  Note that you should expect requests to
    potentially take a long time.

    TODO:  error callback not yet written ... message boxes for the moment

    :param session:  the as-yet-amorphous connection manager
    """

    def __init__(self, session, result_factory):
        self.session = session
        self.result_factory = result_factory

    def future(self, executor):
        future = RequestFuture(self.session, executor)
        future.get = functools.partial(future.outer_get, self)
        return future

    def get(self, tail, *args, **kwargs):
        tail = tail.format(*args)
        s = self.session
        headers = {}
        if "cancel_token" in kwargs:
            headers["X-Yenot-CancelToken"] = kwargs["cancel_token"]
            del kwargs["cancel_token"]
        r = s.get(s.prefix(tail), headers=headers, params=kwargs)
        if r.status_code != 200:
            raise raise_exception_ex(r, "GET")
        return self.result_factory(r.text)

    def post(self, tail, *args, **kwargs):
        tail = tail.format(*args)
        if "files" in kwargs:
            files = kwargs.pop("files")
        else:
            files = None
        if "data" in kwargs:
            data = kwargs.pop("data")
        else:
            data = None
        s = self.session
        r = s.post(s.prefix(tail), params=kwargs, data=data, files=files)
        if r.status_code != 200:
            raise raise_exception_ex(r, "POST")
        return self.result_factory(r.text)

    def put(self, tail, *args, **kwargs):
        tail = tail.format(*args)
        if "files" in kwargs:
            files = kwargs.pop("files")
        else:
            files = None
        if "data" in kwargs:
            data = kwargs.pop("data")
        else:
            data = None
        s = self.session
        r = s.put(s.prefix(tail), params=kwargs, data=data, files=files)
        if r.status_code != 200:
            raise raise_exception_ex(r, "PUT")
        return self.result_factory(r.text)

    def delete(self, tail, *args, **kwargs):
        tail = tail.format(*args)
        if "files" in kwargs:
            files = kwargs.pop("files")
        else:
            files = None
        s = self.session
        r = s.delete(s.prefix(tail), params=kwargs, files=files)
        if r.status_code != 200:
            raise raise_exception_ex(r, "DELETE")
        return self.result_factory(r.text)
