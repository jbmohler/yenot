import os
import pytest
import yenot.backend
import yenot.backend.api as api


@pytest.fixture
def conn():
    return yenot.backend.create_connection(os.environ["YENOT_DB_URL"])


def test_sql_readers(conn):
    rlist = api.sql_rows(conn, "select 1")
    assert len(rlist) == 1

    one = api.sql_1row(conn, "select 1")
    assert one == 1

    obj = api.sql_1object(conn, "select 1 as one, 2 as two")
    assert obj.one == 1
    assert obj.two == 2

    params = {"x": "ex", "y": "why"}
    obj = api.sql_1object(conn, "select %(x)s as col1, %(y)s as col2", params)
    assert obj.col1 == "ex"
    assert obj.col2 == "why"
