from bottle import request
import yenot.backend.api as api

app = api.get_global_app()

@app.get('/api/test/sql-exception01', name='get_api_test_sql_exception01')
def get_api_test_sql_exception01():
    select = """
select *
from nonexistent
"""

    results = api.Results()
    with app.dbconn() as conn:
        results.tables['data', True] = api.sql_tab2(conn, select)
    # exception throw, will never get here ... happy to present as error if it
    # does get here.
    #return results.json_out()

@app.get('/api/test/sql-exception02', name='get_api_test_sql_exception02')
def get_api_test_sql_exception02():
    select = """
select *
from nonexistent syntax error
"""

    results = api.Results()
    with app.dbconn() as conn:
        results.tables['data', True] = api.sql_tab2(conn, select)
    # exception throw, will never get here ... happy to present as error if it
    # does get here.
    #return results.json_out()

@app.get('/api/test/user-error', name='get_api_test_user_error')
def get_api_test_user_error():
    raise api.UserError('test', 'Test error human readable message.')

@app.get('/api/test/python-error', name='get_api_test_python_error')
def get_api_test_python_error():
    no_function_by_this_name()
