import yenot.backend.api as api

app = api.get_global_app()


@app.get("/api/database/stats", name="get_api_database_stats")
def get_api_database_stats():
    results = api.Results()
    with app.dbconn() as conn:
        results.keys["sql_version"] = api.sql_1row(conn, "select version()")
    return results.json_out()


@app.get(
    "/api/database/relation-sizes",
    name="get_api_database_relation_sizes",
    report_title="Relation Sizes on Disk",
)
def get_api_database_relation_sizes():
    select = """
select nspname || '.' || relname as relation,
    pg_size_pretty(pg_relation_size(c.oid)) as size,
    pg_relation_size(c.oid) as size_bytes
from pg_class c
left join pg_namespace n on (n.oid = c.relnamespace)
where nspname not in ('pg_catalog', 'information_schema')
order by pg_relation_size(c.oid) desc
"""

    results = api.Results(default_title=True)
    with app.dbconn() as conn:
        results.tables["sizes", True] = api.sql_tab2(conn, select)
    return results.json_out()


@app.get(
    "/api/database/connections",
    name="get_api_database_connections",
    report_title="Current Database Connections",
)
def get_api_database_connections():
    select = """
SELECT a.datname, a.pid, a.usename, a.application_name, 
    a.client_addr, a.client_hostname, a.client_port, 
    a.backend_start at time zone 'utc' as backend_start,
    a.xact_start at time zone 'utc' as xact_start,
    a.query_start at time zone 'utc' as query_start,
    a.state,
    a.query
FROM pg_stat_activity a;
"""

    results = api.Results(default_title=True)
    with app.dbconn() as conn:
        results.tables["connections", True] = api.sql_tab2(conn, select)
    return results.json_out()


@app.get(
    "/api/database/locks",
    name="get_api_database_locks",
    report_title="Current Database Locks",
)
def get_api_database_locks():
    select = """
SELECT 
    a.datname,
    c.relname,
    l.transactionid,
    l.mode,
    l.GRANTED,
    a.usename,
    a.query, 
    a.query_start at time zone 'utc' as start_time, 
    --age(now(), a.query_start) AS "age", 
    a.pid 
FROM pg_stat_activity a
JOIN pg_locks l ON l.pid = a.pid
JOIN pg_class c ON c.oid = l.relation
ORDER BY a.query_start;
"""

    results = api.Results(default_title=True)
    with app.dbconn() as conn:
        results.tables["locks", True] = api.sql_tab2(conn, select)
    return results.json_out()


@app.put("/api/database/cancelbackend", name="put_api_database_cancelbackend")
def put_api_database_cancelbackend(request):
    pid = request.params.get("pid")

    with app.dbconn() as conn:
        api.sql_void(conn, "select pg_cancel_backend(%(p)s)", {"p": pid})
    return api.Results().json_out()
