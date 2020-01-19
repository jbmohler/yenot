from bottle import request
import yenot.backend.api as api

app = api.get_global_app()


@app.get("/api/test/http-method-get", name="get_api_test_http_method_get")
def get_api_test_http_method_get():
    return "."


@app.post("/api/test/http-method-post", name="post_api_test_http_method_post")
def post_api_test_http_method_post():
    return "."


@app.put("/api/test/http-method-put", name="put_api_test_http_method_put")
def put_api_test_http_method_put():
    return "."


@app.delete("/api/test/http-method-delete", name="delete_api_test_http_method_delete")
def delete_api_test_http_method_delete():
    return "."
