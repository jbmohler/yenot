from bottle import request
import yenot.backend.api as api

app = api.get_global_app()


@app.get("/api/ping", name="ping", skip=["yenot-auth"])
def ping():
    return "."


@app.put("/api/request/cancel", name="api_request_cancel")
def api_request_cancel():
    token = request.query.get("token")
    app.cancel_request(token)
    return api.Results().json_out()
