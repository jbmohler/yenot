from bottle import request
import yenot.backend.api as api

app = api.get_global_app()


@app.get("/api/test/read-sitevar", name="api_test_read_sitevar")
def get_test_read_sitevar():
    sitevar_key = request.query.get("key")

    if sitevar_key not in app.sitevars:
        raise api.UserError("invalid-key", f"No site variable known by {sitevar_key}")

    results = api.Results()
    results.keys["value-get"] = app.sitevars.get(sitevar_key)
    return results.json_out()
