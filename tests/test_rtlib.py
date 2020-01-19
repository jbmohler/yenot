import rtlib


def test_fixedrecord_errors():
    try:
        rtlib.fixedrecord("Test", ["asdf", "45678"])
    except Exception as e:
        assert str(e).find("must be valid") > 0
    try:
        rtlib.fixedrecord("Test", ["asdf", "class"])
    except Exception as e:
        assert str(e).find("must not be keywords") > 0


def test_fixedrecord_access():
    Person = rtlib.fixedrecord("Person", ["name", "age"])
    joel = Person("Joel", 40)
    assert "name" in joel._as_dict()
    assert "Joel" in joel._as_tuple()


def test_rttable():
    table = rtlib.simple_table(["name", "age"])
    with table.adding_row() as row:
        row.name = "Joel"
        row.age = 40

    t2 = table.as_tab2()
    assert len(t2[0]) == 2  # 2 columns
    assert len(t2[1]) == 1  # 1 row
