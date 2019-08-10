from bottle import request
import yenot.backend.api as api

app = api.get_global_app()

@app.put('/api/server/shutdown', name='api_server_shutdown', skip=['yenot-auth'])
def api_server_shutdown():
    app.delayed_shutdown()
    return api.Results().json_out()

@app.get('/api/request/sleep', name='api_request_sleep', skip=['yenot-auth'])
def api_request_sleep():
    duration = api.parse_float(request.query.get('duration'))

    select = """
select pg_sleep(%s)
"""

    results = api.Results()
    with app.dbconn() as conn:
        results.tables['sleep'] = api.sql_tab2(conn, select, (duration,))
    return results.json_out()
