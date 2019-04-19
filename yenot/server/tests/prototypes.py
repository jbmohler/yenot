from bottle import request
import rtlib
import yenot.backend.api as api

app = api.get_global_app()

@app.get('/api/test/prototype', name='api_test_prototype')
def get_test_prototype():
    select = """
select 'ABCDEF0123456789'::char(16) as id, 
    23 as number,
    'Fred'::varchar(50) as name,
    3.14::numeric(12, 2) as pi,
    false as bool_column
"""

    results = api.Results()
    results.key_labels += 'Example Title'
    results.keys['scalar'] = 23
    with app.dbconn() as conn:
        results.tables['data', True] = api.sql_tab2(conn, select)

    return results.json_out()

@app.get('/api/test/date-columns', name='api_test_date_columns')
def get_test_date_columns():
    select = """
select current_date as today,
    current_timestamp at time zone 'utc' as now,
    null::date as nulldate,
    null::timestamp as nullts
"""

    results = api.Results()
    with app.dbconn() as conn:
        results.tables['data', True] = api.sql_tab2(conn, select)

    return results.json_out()

@app.get('/api/test/parse-types', name='api_test_parse_types')
def get_test_parse_types():
    myfloat = api.parse_float(request.query.get('myfloat'))
    mydate = api.parse_date(request.query.get('mydate'))
    mybool = api.parse_bool(request.query.get('mybool'))
    myint = api.parse_int(request.query.get('myint'))

    return api.Results().json_out()

@app.get('/api/test/modify-table', name='api_test_modify_table')
def get_test_modify_table():
    select = """
select 'Joel'::varchar(30) as name,
    40::integer as age
"""

    results = api.Results()
    with app.dbconn() as conn:
        rawdata = api.sql_tab2(conn, select)

        table = rtlib.UnparsingClientTable(*rawdata)
        for row in table.rows:
            row.age += 1

        results.tables['data'] = table.as_tab2()

    return results.json_out()