import yenot.backend.api as api

app = api.get_global_app()


# TODO:  This provides diagnostics for path translation


@app.get("/proxytest/page", name="get_proxytest_page", skip=["yenot-auth"])
def get_proxytest_page(request):
    template = f"""\
<html>
 <head>
  <title>Proxy Test Page</title>
 </head>
 <body>
  <h1>Proxy Test Page</h1>
  <p>
  <h2>External Paths</h2>
  <ul>
  <li><b>Scheme:</b>  </li>
  <li><b>Host/Port:</b>  {request.environ.get('HTTP_X_FORWARDED_FOR')}</li>
  <li><b>Base:</b>  {request.environ.get('YENOT_BASE_URL')}</li>
  <li><b>This Page:</b>  {app}</li>
  </ul>
  </p>
  <p>
  <h2>Internal Paths</h2>
  <ul>
  <li><b>Scheme:</b>:  </li>
  <li><b>Host/Port:</b>:  </li>
  <li><b>Base:</b>  </li>
  <li><b>This Page:</b>  {request.url}</li>
  </ul>
  </p>
 </body>
</html>
"""

    return template
