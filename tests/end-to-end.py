import os
import sys
import time
import concurrent.futures as futures
import rtlib
import yenot.client as yclient
import yenot.tests

TEST_DATABASE = 'yenot_e2e_test'

def test_url(dbname):
    return 'postgresql://localhost/{}'.format(dbname)

def init_database(dburl):
    r = os.system('{} ./scripts/init-database.py {} --full-recreate --ddl-script=#yenotroot#/tests/item.sql --user=admin'.format(sys.executable, dburl))
    if r != 0:
        print('error exit')
        sys.exit(r)

def test_server_info(dburl):
    with yenot.tests.server_running(dburl) as server:
        session = yclient.YenotSession(server.url)

        client = session.raw_client()
        client.get('api/test/http-method-get')
        client.post('api/test/http-method-post')
        client.put('api/test/http-method-put')
        client.delete('api/test/http-method-delete')

        client = session.std_client()

        client.get('api/test/parse-types', 
                        myfloat=3.1415926535,
                        myint=64,
                        mybool=True,
                        mydate='2019-01-01')

        something = client.get('api/test/prototype')
        assert something.keys['scalar'] == 23
        assert something.named_table('data').rows[0].name == 'Fred'
        something = client.get('api/test/date-columns')
        something.named_table('data')
        something = client.get('api/test/modify-table')
        something.named_table('data')

        try:
            client.get('api/test/sql-exception01')
        except yclient.YenotServerError as e:
            assert str(e).find('with status code') > 0

        try:
            client.get('api/test/sql-exception02')
        except yclient.YenotServerError as e:
            assert str(e).find('with status code') > 0

        try:
            client.get('api/test/python-error')
        except yclient.YenotServerError as e:
            assert str(e).find('with status code') > 0

        try:
            client.get('api/test/user-error')
        except yclient.YenotError as e:
            assert str(e).find('human') > 0

        inbound = rtlib.simple_table(['id', 'xint'])
        with inbound.adding_row() as row:
            row.id = 'x1'
            row.xint = 1
        with inbound.adding_row() as row:
            row.id = 'x2'
            row.xint = 2
        out = client.post('api/test/receive-table', 
                    files={'inbound': inbound.as_http_post_file()})
        assert out.named_table('outbound').rows[0].xint == 2
        try:
            t = rtlib.simple_table(['id', 'not_good'])
            out = client.post('api/test/receive-table', 
                        files={'inbound': t.as_http_post_file()})
        except Exception as e:
            assert str(e).find('contains incorrect data')
        try:
            t = rtlib.simple_table(['id'])
            out = client.post('api/test/receive-table', 
                        files={'inbound': t.as_http_post_file()})
        except Exception as e:
            assert str(e).find('fields not given')

        # for sake of coverage, quick call
        t1 = time.time()
        with futures.ThreadPoolExecutor(max_workers=1) as executor:
            fsleep = client.future(executor).get('api/request/sleep', duration=4)
            time.sleep(.5) # give a bit of time for the server
            fsleep.cancel()
            try:
                fsleep.result()
            except Exception as e:
                assert str(e).find('cancelled request') > 0
        t2 = time.time()
        assert t2-t1 < 1

        t1 = time.time()
        with futures.ThreadPoolExecutor(max_workers=2) as executor:
            fsleep1 = client.future(executor).get('api/request/sleep', duration=.4)
            fsleep2 = client.future(executor).get('api/request/sleep', duration=.4)
            sleeps = fsleep1.result(), fsleep2.result()
            assert sleeps[0].named_table('sleep') != None
            assert sleeps[1].named_table('sleep') != None
        t2 = time.time()
        assert .4 < t2-t1 < .6, 'timed out -- {}, {}'.format(t2, t1)

        client.get('api/sql/info')

class ItemMixin:
    def _rtlib_init_(self):
        self.this_init = True

    def fancy_label(self):
        return 'Item : {}\nPrice: ${:.02f}'.format(self.name, self.price)

    @property
    def client_class(self):
        if self.price > 1000:
            return 'A'
        elif self.price > 100:
            return 'B'
        elif self.price > 10:
            return 'C'
        else:
            return 'D'

def test_read_write(dburl):
    with yenot.tests.server_running(dburl) as server:
        session = yclient.YenotSession(server.url)
        client = session.std_client()

        # do some reading and writing
        client.post('api/test/item', name='Table', price=35)
        try:
            client.post('api/test/item', name='Table', price=35)
        except Exception as e:
            assert str(e).find('duplicate key') > 0
        try:
            client.post('api/test/item', name='Sofa')
        except Exception as e:
            assert str(e).find('non-empty and valid') > 0

        data = client.get('api/test/items/all')
        items = data.main_table(mixin=ItemMixin)
        assert items.rows[0].fancy_label().startswith('Item')

        client.put('api/test/update-item', 
                    files={'item': items.as_http_post_file(inclusions=['name', 'price', 'client_class'])})

if __name__ == '__main__':
    init_database(test_url(TEST_DATABASE))
    test_server_info(test_url(TEST_DATABASE))
    test_read_write(test_url(TEST_DATABASE))
