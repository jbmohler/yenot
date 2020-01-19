import yenot.backend.api as api

app = api.get_global_app()


@app.get("/api/sql/info", name="api_sql_info")
def api_sql_info():
    results = api.Results()
    # results.keys['yenot_version'] = str(yenot.__version_info__)
    with app.dbconn() as conn:
        results.keys["sql_version"] = api.sql_1row(conn, "select version()")
    return results.json_out()
