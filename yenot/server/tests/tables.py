from bottle import request
import yenot.backend.api as api

app = api.get_global_app()

@app.post('/api/test/receive-table', name='post_api_test_receive_table')
def post_api_test_receive_table():
    incoming = api.table_from_tab2('inbound', required=['id', 'xint'])

    for row in incoming.rows:
        row.xint += 1

    results = api.Results()
    results.tables['outbound'] = incoming.as_tab2()
    return results.json_out()
