import os
import argparse
import getpass
import bcrypt
import urllib.parse
import psycopg2
import psycopg2.extensions
import psycopg2.extras
import psycopg2.sql

class InitError(Exception):
    pass

def drop_db(dburl):
    result = urllib.parse.urlsplit(dburl)
    # on a journey here, but the point is that the dbname should be from the URL
    dbname = result.path[1:]
    if result.password != None:
        # current user based auth won't have an embedded password
        os.environ['PGPASSWORD'] = result.password
    if result.username != None:
        os.system('dropdb -U {} {}'.format(result.username, dbname))
    else:
        os.system('dropdb {}'.format(dbname))

def test_and_create_db(dburl):
    result = urllib.parse.urlsplit(dburl)
    if result.scheme != 'postgresql':
        raise InitError('only postgresql is supported as a backend database')
    if result.path[0] != '/':
        raise InitError('no path found')
    dbname = result.path[1:]
    if dbname == 'postgres':
        raise InitError('cannot overwrite system table postgres')

    admin_kwargs = {'dbname': 'postgres'}
    if result.port != None:
        admin_kwargs['port'] = result.port
    if result.username != None:
        admin_kwargs['user'] = result.username
    if result.password != None:
        admin_kwargs['password'] = result.password
    admin_kwargs['cursor_factory'] = psycopg2.extras.NamedTupleCursor

    conn_admin = psycopg2.connect(**admin_kwargs)
    conn_admin.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

    with conn_admin.cursor() as c:
        c.execute("select datname from pg_database")
        dbnames = [row.datname for row in c.fetchall()]
        if dbname in dbnames:
            raise InitError('database {} already exists'.format(dbname))

    with conn_admin.cursor() as c:
        c.execute("select version()")
        x = [row.version for row in c.fetchall()]
        print('PostgreSQL version:  {}'.format(x[0]))

    with conn_admin.cursor() as c:
        c.execute(psycopg2.sql.SQL('create database {}').format(psycopg2.sql.Identifier(dbname)))
    conn_admin.close()

def create_connection(dburl):
    result = urllib.parse.urlsplit(dburl)

    dbname = result.path[1:]

    kwargs = {'dbname': result.path[1:]}
    if result.port != None:
        kwargs['port'] = result.port
    if result.username != None:
        kwargs['user'] = result.username
    if result.password != None:
        kwargs['password'] = result.password
    kwargs['cursor_factory'] = psycopg2.extras.NamedTupleCursor

    return psycopg2.connect(**kwargs)

def create_schema(conn, ddlfiles):
    rootdir = os.path.normpath(__file__)
    rootdir = os.path.dirname(rootdir)
    ddlfiles = ['#yenotroot#/core.sql'] + ddlfiles[:]
    root = os.path.normpath(os.path.join(rootdir, '..', 'schema'))

    for ddl in ddlfiles:
        fname = ddl
        if fname.startswith('#yenotroot#'):
            fname = fname.replace('#yenotroot#', root)
        print('Load SQL script {}'.format(fname))
        try:
            with conn.cursor() as c, open(fname, 'r') as sqlfile:
                sql = sqlfile.read()
                c.execute(sql)
                conn.commit()
        except Exception as e:
            print('Error loading {} -- {}'.format(ddl, str(e)))
            sys.exit(1)

if __name__ == '__main__':
    parse = argparse.ArgumentParser('initialize a pyhacc database')
    parse.add_argument('dburl', help='database identifier in url form (e.g. postgresql://user@host/dbname)')
    parse.add_argument('--full-recreate', default=False, action='store_true', 
            help='drop and recreate the database')
    parse.add_argument('--ddl-script', 
            action='append', default=[],
            help='extra sql ddl initialization scripts')
    parse.add_argument('--user', default=None, help='administrative user name (no user created if not supplied)')

    args = parse.parse_args()

    if args.full_recreate:
        drop_db(args.dburl)
    test_and_create_db(args.dburl)
    with create_connection(args.dburl) as conn:
        create_schema(conn, args.ddl_script)
        #load_essentials(conn)
        #if args.user != None:
        #    if os.environ.get('INIT_DB_PASSWD', None) != None:
        #        pw = os.environ['INIT_DB_PASSWD']
        #    else:
        #        pw = getpass.getpass('Password for {}: '.format(args.user))
        #    create_pyhacc_user(conn, args.user, pw)
