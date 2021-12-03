import os
import yenot.backend.api as api
import requests

app = api.get_global_app()


@app.get(
    "/api/monitor/ping-canary", name="get_api_monitor_ping_canary", skip=["yenot-auth"]
)
def get_api_monitor_ping_canary():
    requests.get(os.getenv("EXTERNAL_CANARY_URL"))
    return api.Results().json_out()
