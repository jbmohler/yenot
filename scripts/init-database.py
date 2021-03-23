import os
import argparse
import importlib
import urllib.parse
import psycopg2
import psycopg2.extensions
import psycopg2.errors
import psycopg2.extras
import psycopg2.sql
import yenot.backend


class InitError(Exception):
    pass


def admin_connection(dburl):
    result = urllib.parse.urlsplit(dburl)
    if result.scheme not in ("postgresql", "postgres"):
        raise InitError("only postgresql is supported as a backend database")
    if result.path[0] != "/":
        raise InitError("no path found")
    dbname = result.path[1:]
    if dbname == "postgres":
        raise InitError("cannot overwrite system table postgres")

    admin_kwargs = {"dbname": "postgres"}
    if result.hostname not in [None, ""]:
        admin_kwargs["host"] = result.hostname
    if result.port != None:
        admin_kwargs["port"] = result.port
    if result.username != None:
        admin_kwargs["user"] = result.username
    if result.password != None:
        admin_kwargs["password"] = result.password
    admin_kwargs["cursor_factory"] = psycopg2.extras.NamedTupleCursor

    conn_admin = psycopg2.connect(**admin_kwargs)
    conn_admin.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    return conn_admin


def drop_db(dburl):
    result = urllib.parse.urlsplit(dburl)
    dbname = result.path[1:]

    conn_admin = admin_connection(dburl)
    try:
        with conn_admin.cursor() as c:
            c.execute(
                psycopg2.sql.SQL("drop database {}").format(
                    psycopg2.sql.Identifier(dbname)
                )
            )
    except psycopg2.errors.InvalidCatalogName:
        # no database here to delete
        pass
    except Exception as e:
        print("Unexpected error removing database:  ", e)
    conn_admin.close()


def test_and_create_db(dburl):
    result = urllib.parse.urlsplit(dburl)
    dbname = result.path[1:]

    conn_admin = admin_connection(dburl)

    with conn_admin.cursor() as c:
        c.execute("select datname from pg_database")
        dbnames = [row.datname for row in c.fetchall()]
        if dbname in dbnames:
            raise InitError(
                f"database {dbname} already exists (consider --full-recreate)"
            )

    with conn_admin.cursor() as c:
        c.execute("select version()")
        x = [row.version for row in c.fetchall()]
        print(f"PostgreSQL version:  {x[0]}")

    with conn_admin.cursor() as c:
        c.execute(
            psycopg2.sql.SQL("create database {}").format(
                psycopg2.sql.Identifier(dbname)
            )
        )
    conn_admin.close()


def create_schema(conn, ddlfiles):
    rootdir = os.path.normpath(__file__)
    rootdir = os.path.dirname(rootdir)
    ddlfiles = ["#yenotroot#/core.sql"] + ddlfiles[:]
    root = os.path.normpath(os.path.join(rootdir, "..", "schema"))

    for ddl in ddlfiles:
        fname = ddl
        if fname.startswith("#yenotroot#"):
            fname = fname.replace("#yenotroot#", root)
        print(f"Load SQL script {fname}")
        try:
            with conn.cursor() as c, open(fname, "r") as sqlfile:
                sql = sqlfile.read()
                c.execute(sql)
                conn.commit()
        except Exception as e:
            raise InitError(f"Error loading {ddl} -- {str(e)}")


if __name__ == "__main__":
    parse = argparse.ArgumentParser("initialize a yenot database")
    parse.add_argument(
        "dburl",
        help="database identifier in url form (e.g. postgresql://user@host/dbname)",
    )
    parse.add_argument(
        "--full-recreate",
        default=False,
        action="store_true",
        help="drop and recreate the database",
    )
    parse.add_argument(
        "--ddl-script",
        action="append",
        default=[],
        help="extra sql ddl initialization scripts",
    )
    parse.add_argument(
        "--module",
        action="append",
        default=[],
        help="specify module to import before starting yenot server",
    )
    parse.add_argument(
        "--user",
        default=None,
        help="administrative user name (no user created if not supplied)",
    )

    args = parse.parse_args()

    if args.full_recreate:
        drop_db(args.dburl)
    test_and_create_db(args.dburl)
    with yenot.backend.create_connection(args.dburl) as conn:
        create_schema(conn, args.ddl_script)

        import yenot.backend

        app = yenot.backend.init_application(args.dburl)

        import yenot.server

        for m in args.module:
            importlib.import_module(m)

        import yenot.backend.api as api

        for func in api.app_init_functions:
            func(app)
        for func in api.data_init_functions:
            print(f"running data init {func.__name__}")
            func(conn, args)
