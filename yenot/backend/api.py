from bottle import request, response
import rtlib
from . import misc

sql_tab2 = misc.sql_tab2
sql_1row = misc.sql_1row
sql_void = misc.sql_void
UserError = misc.UserError
table_from_tab2 = misc.table_from_tab2

parse_date = rtlib.parse_date
parse_bool = rtlib.parse_bool
parse_int = lambda x: int(x) if x != None else None
parse_float = lambda x: float(x) if x != None else None

def get_global_app():
    from . import plugins
    return plugins.global_app

class Results:
    """
    This class codifies the expected output of Yenot standard json.  A number if
    syntactic operator based tricks are employed to enable elegant server-side
    endpoint code.

    Q:  Are the operator tricks in this code too crafty?  Does that indicate
    that the client code in the server end-points will be obscured by unusual
    semantics?
    """
    def __init__(self, default_title=False):
        self.keys = {'headers': []}
        self._main_name = None
        self._t = {}

    @property
    def key_labels(self):
        """
        The key_labels property provides syntactically convenient
        method of appending a string to the self.keys['headers'] list.
        This property's sole purpose is the '+=' operator.

        .. code-block:: python

            results = api.Results()
            results.key_labels += 'Title 1'
            results.key_labels += 'Title 2'
        """
        class _:
            def __iadd__(_self, other):
                self.keys['headers'] += [other]

        return _()

    @key_labels.setter
    def key_labels(self, _):
        # make this property readonly
        pass

    @property
    def tables(self):
        # Support:
        # results.tables['<tname>'] = api.sql_tab2(...)
        class _:
            def __setitem__(_self, index, value):
                main = False
                tname = index
                if isinstance(index, tuple):
                    assert len(index) == 2
                    tname, main = index
                self._t[tname] = value
                if main == True:
                    self._main_name = tname

            def __getitem__(_self, index):
                return self._t[index]

        return _()

    def finalize(self):
        if 'summary' not in self.keys and self._main_name != None:
            self.keys['summary'] = '{:,} rows'.format(len(self._t[self._main_name][1]))

    def plain_old_python(self):
        self.finalize()

        assert len(set(self.keys).intersection(set(self._t))) == 0, 'table names & key names cannot overlap'

        tables = self._t.copy()

        keys = self.keys.copy()
        keys.update(tables)
        keys['__main_table__'] = self._main_name
        return keys

    def json_out(self):
        """
        Set the bottle response header content type and flatten the values in
        this object to the Yenot JSON format.  Typically this is used as the
        return value of a JSON returning end-point.

        .. code-block:: python

            results = api.Results()
            return results.json_out()
        """
        response.content_type = 'application/json; charset=UTF-8'
        pyobj = self.plain_old_python()
        return rtlib.serialize(pyobj).encode('utf-8')
