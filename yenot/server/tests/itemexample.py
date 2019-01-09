from bottle import request
import rtlib
import yenot.backend.api as api

app = api.get_global_app()

@app.get('/api/test/items/all', name='get_api_test_items_all')
def get_api_test_items_all():
    results = api.Results()
    with app.dbconn() as conn:
        results.tables['items', True] = api.sql_tab2(conn, "select * from item")
        for row in results.tables['items'][1]:
            pass
    return results.json_out()

@app.post('/api/test/item', name='post_api_test_item')
def post_api_test_item():
    name = request.query.get('name')
    price = api.parse_float(request.query.get('price', None))

    insert = """
insert into item (name, price)
values (%(n)s, %(p)s)"""

    with app.dbconn() as conn:
        api.sql_void(conn, insert, {'n': name, 'p': price})
        conn.commit()
    return api.Results().json_out()

@app.put('/api/test/update-item', name='put_api_test_update_item')
def put_api_test_update_item():
    item = api.table_from_tab2('item', required=['name', 'price'], options=['client_class'], amendments=['server_class'])

    # Save code might go here, but this is a strange http level API.  Here we
    # just test a few things for the sake of coverage.
    for row in item.rows:
        assert str(row).find('unassigned') > 0
    return api.Results().json_out()
