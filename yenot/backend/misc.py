import json
#import sitesql
import rtlib
from bottle import request
import psycopg2.extras as extras
import psycopg2.extensions as psyext

class UserError(Exception):
    def __init__(self, key, msg):
        super(UserError, self).__init__(msg)
        self.key = key


def write_event_entry(conn, ltype, ldescr, ldata):
    ins = """
insert into yenotsys.eventlog (logtype, logtime, descr, logdata)
values (%(lt)s, current_timestamp, %(ld)s, %(lj)s)
returning id, logtype, logtime;"""
    dumps = lambda x: json.dumps(x, cls=rtlib.DateTimeEncoder)
    with conn.cursor(cursor_factory=extras.NamedTupleCursor) as cursor:
        cursor.execute(ins, {'lt': ltype, 'ld': ldescr, 'lj': extras.Json(ldata, dumps=dumps)})
        row = list(cursor.fetchall())[0]
    return row.id, row.logtype, row.logtime

def sql_1row(conn, select, params=None):
    """
    Note that this function is designed to be always used with tuple unpacking
    for multiple columns and the single value is unpacked in the function.
    Thus the function returns the actual single column value if a single column
    and a tuple otherwise.  While this decision looks awkward at this level, it
    is convenient on the outside.

    >>> import sitesql
    >>> conn = sitesql.rtx_sql_conn('test')
    >>> one = sql_1row(conn, "select 1")
    >>> one
    1
    >>> one, two = sql_1row(conn, "select 1, 2")
    >>> one
    1
    >>> two
    2
    """
    # The presence of non-none params in the call to execute causes psycopg2
    # interpolation.   This may or may not be desirable in general.
    if params == None:
        params = []
    # use simple tuple cursor no matter what the connection cursor_factory is.
    cursor = conn.cursor(cursor_factory=psyext.cursor)

    cursor.execute(select, params)
    results = list(cursor.fetchall())
    if len(results) == 0:
        row = (None,)*len(cursor.description)
    elif len(results) == 1:
        row = results[0]
    else:
        raise RuntimeError('Multiple row result in sql_1row')

    cursor.close()
    # This is moderately ugly semantic decision here.  If you don't like it,
    # don't use this function :).
    return row[0] if len(row) == 1 else row

def sql_void(conn, sql, params=None):
    """
    Execute an SQL statement.  You must call `conn.commit()` after this
    function for the change to be committed.
    """
    # The presence of non-none params in the call to execute causes psycopg2
    # interpolation.   This may or may not be desirable in general.
    if params == None:
        params = []
    with conn.cursor() as cursor:
        cursor.execute(sql, params)

def sql_tab2(conn, stmt, mogrify_params=None, column_map=None):
    """
    This convenience function executes an SQL statement and returns a standard
    (columns, rows) tuple prepared to be returned from a Yenot REST end-point.
    The column list is prepared from the SQL columns in the result set.  The
    types are deduced from the SQL result types and the columns are refined by
    the column_map.

    :param connection conn: a database connection object
    :param str stmt: SQL statement to be executed (likely with placeholders for substitution)
    :param dict/tuple mogrify_params: tuple or dictionary to substitute in stmt
    :param dict column_map: a dictionary of column names to rtlib column declaration dictionaries
    """
    cursor = conn.cursor(cursor_factory=extras.NamedTupleCursor)
    if mogrify_params != None:
        cursor.execute(stmt, mogrify_params)
    else:
        cursor.execute(stmt)
    columns, rows = _sql_tab2_cursor(cursor, column_map)
    cursor.close()
    return columns, rows

def _sql_tab2_cursor(cursor, column_map=None):
    """
    This function returns the rows from the (presumably psycopg2) cursor in the
    format expected by Yenot clients.  Briefly, this format is a 2-tuple with
    the first element a list of columns and the second element a list of rows
    with values (no attribute names).  The index of the column in the first
    element maps to the index of the value in each row.
    """
    if column_map == None:
        column_map = {}

    rows = cursor.fetchall()
    columns = []
    for pgcol in cursor.description:
        rt = column_map.get(pgcol[0], {})
        pgtype = pgcol.type_code
        if 'type' not in rt:
            if pgtype in psyext.DATE.values:
                rt['type'] = 'date'
            elif pgtype in psyext.TIME.values+psyext.PYDATETIME.values:
                # Uncertain if this also contains a time-only value
                rt['type'] = 'datetime'
            elif pgtype in psyext.INTEGER.values+psyext.LONGINTEGER.values:
                rt['type'] = 'integer'
            elif pgtype in psyext.FLOAT.values+psyext.DECIMAL.values:
                rt['type'] = 'numeric'
            elif pgtype in psyext.BOOLEAN.values:
                rt['type'] = 'boolean'
        if pgtype in psyext.UNICODE.values and pgcol.internal_size > 0:
            rt['max_length'] = pgcol.internal_size
        columns.append((pgcol[0], rt))
    return (columns, rows)


###### rtlib SERVER incoming UTILS ####


def table_from_tab2(name, required=None, amendments=None, options=None, allow_extra=False):
    try:
        return RecordCollection.from_file(request.files[name].file, \
                        encoding='utf8', \
                        required=required, \
                        amendments=amendments, \
                        options=options, \
                        allow_extra=allow_extra)
    except RuntimeError as e:
        raise UserError('invalid-collection', 'Post file "{}" contains incorrect data.  {}'.format(name, str(e)))

class RecordCollection(rtlib.TypedTable):
    @classmethod
    def from_file(cls, file, encoding='utf8', 
                        required=None, amendments=None, options=None, allow_extra=False):
        payload = json.loads(file.read().decode(encoding))
        keys, fields, rows = payload
        clfields = list(fields)
        allowed = set(options) if options != None else set()
        if required == None:
            required = []
        if required != None:
            allowed = allowed.union(required)
        if amendments != None:
            allowed = allowed.union(amendments)

        if not allow_extra and not set(fields).issubset(allowed):
            raise RuntimeError('Extra fields given:  {}'.format(' '.join(set(fields).difference(allowed))))
        if not set(required).issubset(fields):
            raise RuntimeError('Required fields not given:  {}'.format(' '.join(set(required).difference(fields))))
        if amendments != None:
            clfields += set(amendments).difference(fields)

        dr = rtlib.fixedrecord('DataRow', clfields)
        rows = [dr(**dict(zip(fields, r))) for r in rows]
        self = cls([(c, None) for c in clfields], rows)
        self.DataRow = dr
        self.deleted_keys = keys.get('deleted', [])

        return self
