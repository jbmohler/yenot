#!/usr/bin/env python
import argparse
import importlib
import yenot.backend

if __name__ == '__main__':
    parse = argparse.ArgumentParser('serve a collection of yenot modules')
    parse.add_argument('dburl', 
            help='database identifier in url form (e.g. postgresql://user@host/dbname)')
    parse.add_argument('--module', 
            action='append', default=[],
            help='specify module to import before starting yenot server')

    args = parse.parse_args()

    app = yenot.backend.init_application(args.dburl)

    for m in args.module:
        importlib.import_module(m)

    # debugging & development service
    app.run(debug=True, reloader=False, server=app._paste_server)
