import yenot.backend.api as api

app = api.get_global_app()


@app.get("/api/test/prototype", name="api_test_prototype")
def get_test_prototype():
    select = """
select 'ABCDEF0123456789'::char(16) as id, 
    23 as number,
    'Fred'::varchar(50) as name,
    3.14::numeric(12, 2) as pi,
    false as bool_column
"""

    results = api.Results()
    results.key_labels += "Example Title"
    results.keys["scalar"] = 23
    with app.dbconn() as conn:
        results.tables["data", True] = api.sql_tab2(conn, select)

    return results.json_out()


@app.get("/api/test/date-columns", name="api_test_date_columns")
def get_test_date_columns():
    select = """
select current_date as today,
    current_timestamp at time zone 'utc' as now,
    null::date as nulldate,
    null::timestamp as nullts
"""

    results = api.Results()
    with app.dbconn() as conn:
        results.tables["data", True] = api.sql_tab2(conn, select)

    return results.json_out()


@app.get("/api/test/parse-types", name="api_test_parse_types")
def get_test_parse_types(request):
    myfloat = api.parse_float(request.query.get("myfloat"))
    mydate = api.parse_date(request.query.get("mydate"))
    mybool = api.parse_bool(request.query.get("mybool"))
    myint = api.parse_int(request.query.get("myint"))

    if myfloat < 0.0:
        raise api.UserError("check-value", "no negatives")
    if str(mydate) >= "2020-01-01":
        raise api.UserError("check-value", "no current date")
    if mybool == None:
        raise api.UserError("check-value", "expects legit boolean")
    if myint < 0:
        raise api.UserError("check-value", "no negatives")

    return api.Results().json_out()


@app.get("/api/test/modify-table", name="api_test_modify_table")
def get_test_modify_table():
    select = """
select 'Joel'::varchar(30) as name,
    40::integer as age
"""

    results = api.Results()
    with app.dbconn() as conn:
        rawdata = api.sql_tab2(conn, select)

        def xform_add_one(oldrow, row):
            row.age += 1

        rows = api.tab2_rows_transform(rawdata, rawdata[0], xform_add_one)

        results.tables["data"] = rawdata[0], rows

    return results.json_out()
