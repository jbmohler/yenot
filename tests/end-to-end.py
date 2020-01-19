import os
import sys
import time
import concurrent.futures as futures
import rtlib
import yenot.client as yclient
import yenot.tests


def init_database(dburl):
    r = os.system(
        "{} ./scripts/init-database.py {} --full-recreate --ddl-script=#yenotroot#/tests/item.sql --user=admin".format(
            sys.executable, dburl
        )
    )
    if r != 0:
        print("error exit")
        sys.exit(r)


def test_server_info(dburl):
    with yenot.tests.server_running(dburl) as server:
        session = yclient.YenotSession(server.url)

        client = session.raw_client()
        client.get("api/test/http-method-get")
        client.post("api/test/http-method-post")
        client.put("api/test/http-method-put")
        client.delete("api/test/http-method-delete")

        client = session.std_client()

        client.get(
            "api/test/parse-types",
            myfloat=3.1415926535,
            myint=64,
            mybool=True,
            mydate="2019-01-01",
        )

        something = client.get("api/test/prototype")
        assert something.keys["scalar"] == 23
        assert something.named_table("data").rows[0].name == "Fred"
        something = client.get("api/test/date-columns")
        something.named_table("data")
        something = client.get("api/test/modify-table")
        something.named_table("data")

        try:
            client.get("api/test/sql-exception01")
        except yclient.YenotServerError as e:
            assert str(e).find("with status code") > 0

        try:
            client.get("api/test/sql-exception02")
        except yclient.YenotServerError as e:
            assert str(e).find("with status code") > 0

        try:
            client.get("api/test/python-error")
        except yclient.YenotServerError as e:
            assert str(e).find("with status code") > 0

        try:
            client.get("api/test/user-error")
        except yclient.YenotError as e:
            assert str(e).find("human") > 0

        inbound = rtlib.simple_table(["id", "xint"])
        with inbound.adding_row() as row:
            row.id = "x1"
            row.xint = 1
        with inbound.adding_row() as row:
            row.id = "x2"
            row.xint = 2
        out = client.post(
            "api/test/receive-table", files={"inbound": inbound.as_http_post_file()}
        )
        assert out.named_table("outbound").rows[0].xint == 2
        try:
            t = rtlib.simple_table(["id", "not_good"])
            out = client.post(
                "api/test/receive-table", files={"inbound": t.as_http_post_file()}
            )
        except Exception as e:
            assert str(e).find("contains incorrect data")
        try:
            t = rtlib.simple_table(["id"])
            out = client.post(
                "api/test/receive-table", files={"inbound": t.as_http_post_file()}
            )
        except Exception as e:
            assert str(e).find("fields not given")

        # for sake of coverage, quick call
        t1 = time.time()
        with futures.ThreadPoolExecutor(max_workers=1) as executor:
            fsleep = client.future(executor).get("api/request/sleep", duration=4)
            time.sleep(0.5)  # give a bit of time for the server
            fsleep.cancel()
            try:
                fsleep.result()
            except Exception as e:
                assert str(e).find("cancelled request") > 0
        t2 = time.time()
        assert t2 - t1 < 1

        t1 = time.time()
        with futures.ThreadPoolExecutor(max_workers=2) as executor:
            fsleep1 = client.future(executor).get("api/request/sleep", duration=0.4)
            fsleep2 = client.future(executor).get("api/request/sleep", duration=0.4)
            sleeps = fsleep1.result(), fsleep2.result()
            assert sleeps[0].named_table("sleep") != None
            assert sleeps[1].named_table("sleep") != None
        t2 = time.time()
        assert 0.4 < t2 - t1 < 0.6, "timed out -- {}, {}".format(t2, t1)

        client.get("api/sql/info")


def test_item_crud(dburl):
    with yenot.tests.server_running(dburl) as server:
        session = yclient.YenotSession(server.url)
        client = session.std_client()

        contents = client.get("api/test/item/new")
        item = contents.main_table()
        row = item.rows[0]
        row.name = "Sofa"
        row.price = 4500
        client.post(
            "api/test/item", files={"item": item.as_http_post_file(exclusions=["id"])}
        )

        contents = client.get("api/test/items/list")
        row = [row for row in contents.main_table().rows if row.name == "Sofa"][0]
        contents = client.get("api/test/item/{}/record", row.id)
        assert contents.named_table("items").rows[0].revenue_level == "B"
        item = contents.main_table()
        row = item.rows[0]
        row.name = "Sofa"
        row.price = 450
        client.put(
            "api/test/item/{}",
            row.id,
            files={"item": item.as_http_post_file(exclusions=["id", "revenue_level"])},
        )
        contents = client.get("api/test/item/{}/record", row.id)
        assert contents.named_table("items").rows[0].revenue_level == "C"

        contents = client.get("api/test/items/list")
        assert (
            len([row for row in contents.main_table().rows if row.name == "Sofa"]) == 1
        )
        client.delete("api/test/item/{}", row.id)
        contents = client.get("api/test/items/list")
        assert (
            len([row for row in contents.main_table().rows if row.name == "Sofa"]) == 0
        )


def test_read_write(dburl):
    with yenot.tests.server_running(dburl) as server:
        session = yclient.YenotSession(server.url)
        client = session.std_client()

        # do some reading and writing
        client.post("api/test/item-simple", name="Table", price=35)
        try:
            client.post("api/test/item-simple", name="Table", price=35)
        except Exception as e:
            assert str(e).find("duplicate key") > 0
        try:
            client.post("api/test/item-simple", name="Sofa")
        except Exception as e:
            assert str(e).find("non-empty and valid") > 0

        itable = rtlib.simple_table(["name", "price"])
        with itable.adding_row() as row:
            row.name = "Computer"
            row.price = 30
        client.post("api/test/item", files={"item": itable.as_http_post_file()})

        data = client.get("api/test/items/list")
        items = data.main_table()

        for row in items.rows:
            if row.name == "Table":
                row.price = 81

        client.put(
            "api/test/update-items",
            files={"item": items.as_http_post_file(inclusions=["id", "name", "price"])},
        )

        for row in items.rows:
            row.client_class = "X"
        client.put(
            "api/test/update-item-extras",
            files={
                "item": items.as_http_post_file(
                    inclusions=["name", "price", "client_class"]
                )
            },
        )


def test_sitevar_reads(dburl):
    sitevars = [
        "[group1].[value1]=g1v1",
        "[group1].[value2]=g1v2",
        "[group2].[value1]=g2v1",
    ]
    with yenot.tests.server_running(dburl, sitevars=sitevars) as server:
        session = yclient.YenotSession(server.url)
        client = session.std_client()

        content = client.get("api/test/read-sitevar", key="[group1].[value1]")
        assert content.keys["value-get"] == "g1v1"


if __name__ == "__main__":
    dburl = os.environ["YENOT_DB_URL"]
    init_database(dburl)
    test_server_info(dburl)
    test_item_crud(dburl)
    test_read_write(dburl)
    test_sitevar_reads(dburl)
