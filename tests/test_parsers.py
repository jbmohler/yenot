import datetime
import rtlib

def test_dates():
    assert rtlib.parse_date(None) == None
    assert rtlib.parse_date('2019-01-01') == datetime.date(2019, 1, 1)

def test_bool():
    assert rtlib.parse_bool('TRUE') == True
    assert rtlib.parse_bool(0) == False
    try:
        rtlib.parse_bool('kjw')
    except Exception as e:
        assert str(e).find('unacceptable') >= 0
