import math
import rtlib
import yenot.backend.api as api

app = api.get_global_app()


@app.get("/api/test/item/new", name="get_api_test_item_new")
def get_api_test_item_new():
    select = """
select *
from item
where false"""

    results = api.Results()
    with app.dbconn() as conn:
        columns, rows = api.sql_tab2(conn, select)

        def default_rows(key, row):
            row.price = 12

        rows = api.tab2_rows_default(columns, [None], default_rows)
        results.tables["items", True] = columns, rows
    return results.json_out()


@app.get("/api/test/item/<itemid>/record", name="get_api_test_item_record")
def get_api_test_item_record(itemid):
    select = """
select *
from item
where id=%(item)s"""

    results = api.Results()
    with app.dbconn() as conn:
        results.tables["items", True] = api.sql_tab2(conn, select, {"item": itemid})
        rawdata = results.tables["items"]
        columns = api.tab2_columns_transform(
            rawdata[0], insert=[("price", "revenue_level")]
        )

        def xform_add_rev_level(oldrow, row):
            level = int(math.log10(row.price))
            if level > 4:
                row.revenue_level = "\u221e"
            else:
                row.revenue_level = "EDCBA"[level]

        rows = api.tab2_rows_transform(rawdata, columns, xform_add_rev_level)
        results.tables["items", True] = columns, rows
    return results.json_out()


@app.post("/api/test/item", name="post_api_test_item_record")
def post_api_test_item_record():
    item = api.table_from_tab2("item", required=["name", "price"], allow_extra=True)

    if len(item.rows) != 1:
        raise api.UserError("invalid-params", "Update exactly one row.")
    if "id" in item.DataRow.__slots__:
        raise api.UserError("invalid-params", "id column is banned")

    with app.dbconn() as conn:
        with api.writeblock(conn) as w:
            w.insert_rows("item", item)
        conn.commit()
    return api.Results().json_out()


@app.put("/api/test/item/<itemid>", name="put_api_test_item_record")
def put_api_test_item_record(itemid):
    item = api.table_from_tab2(
        "item", required=["name", "price"], amendments=["id"], allow_extra=True
    )

    if len(item.rows) != 1:
        raise api.UserError("invalid-params", "Update exactly one row.")

    for row in item.rows:
        row.id = itemid

    with app.dbconn() as conn:
        with api.writeblock(conn) as w:
            w.upsert_rows("item", item)
        conn.commit()
    return api.Results().json_out()


@app.delete("/api/test/item/<itemid>", name="delete_api_test_item_record")
def delete_api_test_item_record(itemid):
    with app.dbconn() as conn:
        item = rtlib.simple_table(["id"])
        with item.adding_row() as r2:
            r2.id = int(itemid)
        with api.writeblock(conn) as w:
            w.delete_rows("item", item)
        conn.commit()
    return api.Results().json_out()


@app.get("/api/test/items/list", name="get_api_test_items_list")
def get_api_test_items_list():
    results = api.Results()
    with app.dbconn() as conn:
        cm = api.ColumnMap(
            id=api.cgen.item.surrogate(),
            name=api.cgen.item.name(),
            price=api.cgen.currency_usd(),
            revenue_level=api.cgen.auto(label="rev lev"),
        )
        results.tables["items", True] = api.sql_tab2(
            conn, "select * from item", column_map=cm
        )
    return results.json_out()


@app.post("/api/test/item-simple", name="post_api_test_item_simple")
def post_api_test_item_simple(request):
    name = request.query.get("name")
    price = api.parse_float(request.query.get("price", None))

    insert = """
insert into item (name, price)
values (%(n)s, %(p)s)"""

    with app.dbconn() as conn:
        api.sql_void(conn, insert, {"n": name, "p": price})
        conn.commit()
    return api.Results().json_out()


@app.put("/api/test/update-item-extras", name="put_api_test_update_item")
def put_api_test_update_item():
    item = api.table_from_tab2(
        "item",
        required=["name", "price"],
        options=["client_class"],
        amendments=["server_class"],
    )

    # Save code might go here, but this is a strange http level API.  Here we
    # just test a few things for the sake of coverage.
    for row in item.rows:
        assert str(row).find("unassigned") > 0
    return api.Results().json_out()


@app.put("/api/test/update-items", name="put_api_test_update_items")
def put_api_test_update_items():
    item = api.table_from_tab2("item", required=["id", "name", "price"])

    with app.dbconn() as conn:
        with api.writeblock(conn) as w:
            w.upsert_rows("item", item)
        conn.commit()
    return api.Results().json_out()
