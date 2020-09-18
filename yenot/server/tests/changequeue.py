from bottle import request
import yenot.backend.api as api

app = api.get_global_app()


@app.post("/api/test/changequeue", name="post_api_test_changequeue")
def post_api_test_changequeue():
    channel = request.query.get("channel")

    with app.dbconn() as conn:
        api.sql_void(conn, "notify {}, 'my payload'".format(channel))
        conn.commit()
    return api.Results().json_out()
