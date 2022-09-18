import re
import contextlib
from . import sqlread


PRIM_KEY_SELECT = """
select array_agg(kcu.column_name::text)
from information_schema.table_constraints tc
join information_schema.key_column_usage kcu on
                kcu.constraint_name=tc.constraint_name and
                kcu.table_name=tc.table_name and kcu.table_schema=tc.table_schema
where tc.constraint_type='PRIMARY KEY'
    and tc.table_schema=%(sname)s
    and tc.table_name=%(tname)s
"""

# This returns the foreign key reference information for matrix tables; the
# primary key is expected to be a composite key referencing two different
# tables.
# TODO:  What if there are elements of the primary key which are not a foreign
# key reference?  This query will not return them.
# See $root/schema/yenot.sql for the definition.
MATRIX_KEY_SELECT = """
select * from yenotsys.matrix_key_table(%(sname)s, %(tname)s);
"""

COL_TYPE_SELECT = """
select tables.table_name, columns.column_name, columns.is_nullable,
	columns.data_type, columns.character_maximum_length,
	columns.numeric_precision, columns.numeric_precision_radix, columns.numeric_scale
from information_schema.tables
join information_schema.columns on columns.table_name=tables.table_name and columns.table_schema=tables.table_schema
where tables.table_schema=%(sname)s and tables.table_name=%(tname)s and tables.table_type='BASE TABLE'
"""


class WriteChunk:
    def __init__(self, conn):
        self.conn = conn

    @staticmethod
    def _split_table_name(tname):
        if tname.find(".") >= 0:
            sx, tx = tname.split(".")
        else:
            sx, tx = "public", tname

        if None == re.match("[a-zA-Z_][a-z0-9A-Z_]*", sx):
            raise RuntimeError(
                f'invalid schema name "{sx}" (as determined by regex only)'
            )
        if None == re.match("[a-zA-Z_][a-z0-9A-Z_]*", tx):
            raise RuntimeError(
                f'invalid table name "{tx}" (as determined by regex only)'
            )

        return sx, tx

    def update_rows(self, tname, table, matrix=None):
        # TODO: write this assuring only updates; however, for now just user upsert_rows
        self.upsert_rows(tname, table, matrix=matrix)
        # TODO;  assert there is a row updated for each?

    def insert_rows(self, tname, table, matrix=None):
        if matrix:
            # TODO: this works for now, but I think that upsert really
            # needs to be much more clear about what it is doing --
            # (insert/update/delete) -- and not doing.
            self.upsert_rows(tname, table, matrix)
            return

        sx, tx = WriteChunk._split_table_name(tname)

        insert_sql = """insert into {t} ({columns}) {v}"""

        mog = TableSaveMogrification()
        values = mog.as_values(self.conn, table, table.DataRow.__slots__)
        c = ", ".join(table.DataRow.__slots__)

        with self.conn.cursor() as cursor:
            cursor.execute(insert_sql.format(t=tname, columns=c, v=values))

    def upsert_rows(self, tname, table, matrix=None):
        # TODO -- Perhaps this should be named "upserdel" since it inserts,
        # updates & deletes rows.
        sx, tx = WriteChunk._split_table_name(tname)

        from . import misc

        if not hasattr(table, "deleted_keys"):
            table.deleted_keys = []

        matrix = matrix or {}
        matrix = matrix.copy()
        tosave = set(table.DataRow.__slots__)

        # This loop fulfills the double purpose of restricting the matrix list
        # to actual passed columns and eliminating matrix columns from the base
        # table save-list.
        for k in list(matrix.keys()):
            try:
                tosave.remove(k)

                if isinstance(matrix[k], str):
                    # interpret a single string as this dict
                    matrix[k] = {"table": matrix[k]}
            except KeyError:
                # optional matrix column not included; remove from matrix dict
                del matrix[k]

        def column_cast(schrow, prefix=None):
            """
            Extra a column sql type from the information_schema data_type and
            related columns.

            Returns null if not required to cast (e.g. python string)
            """
            prefix = prefix or ""
            data_type = getattr(schrow, f"{prefix}data_type")
            numeric_precision = getattr(schrow, f"{prefix}numeric_precision")
            numeric_scale = getattr(schrow, f"{prefix}numeric_scale")

            result = None

            if data_type in ("character", "character varying", "text"):
                # no casting necessary
                pass
            elif data_type == "numeric":
                result = f"numeric({numeric_precision}, {numeric_scale})"
            elif data_type in (
                "date",
                "boolean",
                "json",
                "integer",
                "smallint",
                "uuid",
                "bytea",
            ):
                # bit of a catch-all
                result = data_type
            else:
                # untracked for now
                pass

            return result

        cols = sqlread.sql_rows(self.conn, COL_TYPE_SELECT, {"sname": sx, "tname": tx})
        coltypes = {}
        for row in cols:
            if row.column_name in tosave:
                cast_type = column_cast(row)
                if cast_type:
                    coltypes[row.column_name] = cast_type

        my_pkey = sqlread.sql_1row(
            self.conn, PRIM_KEY_SELECT, {"sname": sx, "tname": tx}
        )

        # extract primary keys for this table and cross-check with primary keys
        # of matrix table(s)
        for kmatrix, meta in matrix.items():
            sxm, txm = WriteChunk._split_table_name(meta["table"])

            primkeys = sqlread.sql_1row(
                self.conn, PRIM_KEY_SELECT, {"sname": sxm, "tname": txm}
            )
            if len(primkeys) != 2:
                raise RuntimeError(
                    f"Expecting matrix table {meta['table']} to have 2 column composite primary key (each foreign key)"
                )

            fkeys = sqlread.sql_rows(
                self.conn, MATRIX_KEY_SELECT, {"sname": sxm, "tname": txm}
            )
            if len(fkeys) != 2:
                raise RuntimeError(
                    f"Expecting matrix table {meta['table']} to have 2 foreign key references; found {fkeys}"
                )

            casts = {}
            for fkey in fkeys:
                if fkey.foreign_table_schema == sx and fkey.foreign_table_name == tx:
                    # find the one referencing this table
                    meta["column_self"] = fkey.column_name
                else:
                    # find the one referencing the other table (with type info)
                    meta["column_other"] = fkey.column_name

                cccast = column_cast(fkey, prefix="foreign_")
                if cccast:
                    casts[fkey.column_name] = cccast
            meta["column_types"] = casts

            if not meta.get("column_self") or not meta.get("column_other"):
                raise RuntimeError(
                    f"Expecting matrix table {meta['table']} to point to {sx}.{tx} primary key"
                )

        # run through deletes (first, to drop referencing)
        for kmatrix, meta in matrix.items():
            tmatmeta = table.matrices[kmatrix]
            sxm, txm = WriteChunk._split_table_name(meta["table"])

            columns = [(meta["column_self"], None), (meta["column_other"], None)]
            tremove = misc.InboundTable(columns, [])
            for row in table.rows:
                vmat = getattr(row, kmatrix)
                mykey = getattr(row, my_pkey[0], None)

                if mykey is None:
                    # this is a new row; skip
                    if vmat.get("remove"):
                        raise RuntimeError("new rows cannot remove matrix associations")
                    continue

                # remove things marked for remove
                for r in vmat.get("remove", []):
                    tremove.rows.append(tremove.DataRow(mykey, r))
                # remove things in the universe not in a 'set'
                if "set" in vmat:
                    if "scope" in tmatmeta:
                        # TODO: verify that vmat['set'] is a subset of scope?
                        removes = tmatmeta["scope"].difference(vmat["set"])
                        for r in removes:
                            tremove.rows.append(tremove.DataRow(mykey, r))
                    else:
                        # delete items not in 'set' ?
                        raise RuntimeError(
                            f"Expecting matrix table {meta['table']} to include a matrix scope to set the matrix values"
                        )

            if len(tremove.rows) > 0:
                mat_mog = TableSaveMogrification()
                mat_mog.column_types = meta["column_types"]
                values = mat_mog.as_values(
                    self.conn, tremove, tremove.DataRow.__slots__
                )
                c = ", ".join(tremove.DataRow.__slots__)

                delete_sql = """delete from {t} where ({columns}) in ({v})"""
                with self.conn.cursor() as cursor:
                    cursor.execute(
                        delete_sql.format(t=meta["table"], columns=c, v=values)
                    )

        mog = TableSaveMogrification()
        mog.primary_key = my_pkey
        mog.table = tname
        mog.column_types = coltypes
        mog.persist(self.conn, table, collist=tosave)

        # run through adds (after, to enable references)
        for kmatrix, meta in matrix.items():
            tmatmeta = table.matrices[kmatrix]
            sxm, txm = WriteChunk._split_table_name(meta["table"])

            columns = [(meta["column_self"], None), (meta["column_other"], None)]
            tadd = misc.InboundTable(columns, [])
            for row in table.rows:
                vmat = getattr(row, kmatrix)
                mykey = getattr(row, my_pkey[0])

                if mykey is None:
                    # this is a new row; skip
                    continue

                # remove things marked for remove
                for r in vmat.get("add", []):
                    tadd.rows.append(tadd.DataRow(mykey, r))
                # remove things in the universe not in a 'set'
                if "set" in vmat:
                    # TODO: verify that vmat['set'] is a subset of scope?
                    removes = vmat["set"]
                    for r in removes:
                        tadd.rows.append(tadd.DataRow(mykey, r))

            if len(tadd.rows) > 0:
                mat_mog = TableSaveMogrification()
                mat_mog.column_types = meta["column_types"]
                values = mat_mog.as_values(self.conn, tadd, tadd.DataRow.__slots__)
                c = ", ".join(tadd.DataRow.__slots__)

                insert_sql = """
insert into {t} ({columns}) {v}
on conflict ({cself}, {cother}) do nothing"""
                with self.conn.cursor() as cursor:
                    cursor.execute(
                        insert_sql.format(
                            t=meta["table"],
                            columns=c,
                            v=values,
                            cself=meta["column_self"],
                            cother=meta["column_other"],
                        )
                    )

    def delete_rows(self, tname, table):
        sx, tx = WriteChunk._split_table_name(tname)

        keys = sqlread.sql_1row(self.conn, PRIM_KEY_SELECT, {"sname": sx, "tname": tx})
        if list(sorted(keys)) != list(sorted(table.DataRow.__slots__)):
            raise RuntimeError("primary key must be exactly represented")

        mog = TableSaveMogrification()
        values = mog.as_values(self.conn, table, table.DataRow.__slots__)
        c = ", ".join(table.DataRow.__slots__)

        delete_sql = """delete from {t} where ({columns}) in ({v})"""
        with self.conn.cursor() as cursor:
            cursor.execute(delete_sql.format(t=tname, columns=c, v=values))


@contextlib.contextmanager
def writeblock(conn):
    yield WriteChunk(conn)


def _mogrify_values(cursor, rows, row2dict, columns, types):
    if isinstance(types, dict):
        types = [types.get(cname, None) for cname in columns]
    elif types == None:
        types = [None] * len(columns)
    assert len(types) == len(columns)

    qualnames = [
        f"%({cname})s::{t}" if t != None else f"%({cname})s"
        for cname, t in zip(columns, types)
    ]
    fragment = f"({', '.join(qualnames)})"
    mogrifications = [cursor.mogrify(fragment, row2dict(r)) for r in rows]

    return ",\n\t".join([x.decode(cursor.connection.encoding) for x in mogrifications])


def mogrify_values(cursor, rows, columns, types=None):
    return _mogrify_values(cursor, rows, lambda r: r._as_dict(), columns, types)


def mogrify_values_anon(cursor, rows, columns, types=None):
    return _mogrify_values(
        cursor, rows, lambda r: dict(zip(columns, r)), columns, types
    )


class TableSaveMogrification:
    """
    Consider starting the PG transaction block with::

        set transaction isolation level serializable;
        set constraints all deferred;
    """

    def __init__(self):
        self.table = None
        self.primary_key = None
        self.column_types = None

    def as_values(self, conn, table, columns):
        result_template = """\
values/*REPRESENTED*/
"""

        with conn.cursor() as cursor:
            mogrifications = mogrify_values(
                cursor, table.rows, columns, self.column_types
            )

        return result_template.replace("/*REPRESENTED*/", mogrifications)

    def persist(self, conn, table, collist=None):
        if not collist:
            collist = table.DataRow.__slots__

        if isinstance(self.primary_key, str):
            pkey = [self.primary_key]
        else:
            pkey = self.primary_key

        cols_no_pk = [c for c in collist if c not in pkey]

        colnames = ", ".join([f'"{c}"' for c in collist])
        colnames_no_pk = ", ".join([f'"{c}"' for c in cols_no_pk])
        staging_no_pk = ", ".join([f'staging."{c}"' for c in cols_no_pk])
        colassign = ", ".join(['"{0}"=staging."{0}"'.format(c) for c in cols_no_pk])

        interpolations = {
            "fqtn": self.table,
            "tn": self.table.rsplit(".", 1)[-1],
            "colnames": colnames,
            "colassign": colassign,
            "colnames_no_pk": colnames_no_pk,
            "staging_no_pk": staging_no_pk,
        }

        m1 = ["{tn}.{pk}=staging.{pk}".format(pk=pk, **interpolations) for pk in pkey]
        pkey_match = " and ".join(m1)
        m1 = ["{tn}.{pk} is null".format(pk=pk, **interpolations) for pk in pkey]
        pkey_null = " and ".join(m1)

        interpolations["pkm"] = pkey_match
        interpolations["pkn"] = pkey_null
        interpolations["pkcs"] = ", ".join(pkey)

        insert = """
with staging({colnames}) as (
    values/*REPRESENTED*/
)
insert into {fqtn} ({colnames})
(
    select staging.*
    from staging
    left outer join {fqtn} on {pkm}
    where {pkn}
)""".format(
            **interpolations
        )

        insert2 = """
insert into {fqtn} ({colnames_no_pk})
values/*REPRESENTED*/
returning {pkcs}
""".format(
            **interpolations
        )

        update = """
with staging({colnames}) as (
    values/*REPRESENTED*/
)
update {fqtn} set {colassign} 
from staging
where {pkm}""".format(
            **interpolations
        )

        delete = """
with staging({pknames}) as (
    values/*REPRESENTED*/
)
delete from {fqtn} where ({pknames}) in (select * from staging)""".format(
            pknames=",".join(pkey), **interpolations
        )

        with conn.cursor() as cursor:
            # Delete first since other rows may induce duplicates ... although
            # this raises questions about whether this just moves problems
            # around.  Hence we recommend "set constraints all deferred".
            if len(table.deleted_keys) > 0:
                mogrifications = mogrify_values_anon(
                    cursor, table.deleted_keys, pkey, self.column_types
                )
                my_delete = delete.replace("/*REPRESENTED*/", mogrifications)
                cursor.execute(my_delete, {"keys": tuple(table.deleted_keys)})

            if len(pkey) == 1 and pkey[0] not in table.DataRow.__slots__:
                # the primary key is not even in the insert data; all inserts
                rows1 = []
                rows2 = table.rows
            elif len(pkey) == 1:
                need_defaulting = lambda row: None in [getattr(row, p) for p in pkey]
                rows1 = [row for row in table.rows if not need_defaulting(row)]
                rows2 = [row for row in table.rows if need_defaulting(row)]
            else:
                # defaulting is not supported on composite primary key
                rows1 = table.rows
                rows2 = []
            if len(rows1) > 0:
                mogrifications = mogrify_values(
                    cursor, rows1, collist, self.column_types
                )

                # TODO: consider using upsert
                if len(cols_no_pk) > 0:
                    # update
                    my_update = update.replace("/*REPRESENTED*/", mogrifications)
                    cursor.execute(my_update)
                # insert
                my_insert = insert.replace("/*REPRESENTED*/", mogrifications)
                cursor.execute(my_insert)

            if len(rows2) > 0:
                collist2 = list(collist)
                for pk in pkey:
                    if pk in collist2:
                        collist2.remove(pk)
                mogrifications = mogrify_values(
                    cursor, rows2, collist2, self.column_types
                )
                # insert
                my_insert = insert2.replace("/*REPRESENTED*/", mogrifications)
                cursor.execute(my_insert)
                pkey_inserted = cursor.fetchall()

                # We trust that pkey_inserted is in the same order as rows, but
                # this is a sketchy point.  See
                # https://www.mail-archive.com/pgsql-hackers@postgresql.org/msg253739.html
                # where Craig Ringer makes a case that this should be
                # documented safe.  I'll take him at his word for now.

                if len(pkey_inserted) != len(rows2):
                    raise RuntimeError(
                        f"Inserted into {self.table}:  {len(rows2)} rows; returned {len(pkey_inserted)} rows"
                    )

                for inserted, row in zip(pkey_inserted, rows2):
                    for pk in pkey:
                        setattr(row, pk, getattr(inserted, pk))
