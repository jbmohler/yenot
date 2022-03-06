import yenot.backend.api as api


app = api.get_global_app()


@app.put("/api/sql/changequeue", name="put_api_sql_changequeue")
def put_api_sql_changequeue(request):
    return api.start_listener(request, request.query.get("channel"))


@app.get("/api/sql/changequeue", name="get_api_sql_changequeue")
def get_api_sql_changequeue(request):
    return api.poll_listener(request, request.query.get("channel"))
