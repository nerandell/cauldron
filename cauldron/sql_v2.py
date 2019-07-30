"""

"""

from asyncio import coroutine
from contextlib import contextmanager
from functools import wraps
from enum import Enum
import aiopg
import asyncio
import logging

from aiopg import create_pool, Pool, Cursor

import psycopg2

_CursorType = Enum('CursorType', 'PLAIN, DICT, NAMEDTUPLE')


def dict_cursor(func):
    """
    Decorator that provides a dictionary cursor to the calling function

    Adds the cursor as the second argument to the calling functions

    Requires that the function being decorated is an instance of a class or object
    that yields a cursor from a get_cursor(cursor_type=CursorType.DICT) coroutine or provides such an object
    as the first argument in its signature

    Yields:
        A client-side dictionary cursor
    """

    @wraps(func)
    def wrapper(cls, *args, **kwargs):
        with (yield from cls.get_cursor(_CursorType.DICT)) as c:
            return (yield from func(cls, c, *args, **kwargs))

    return wrapper


def cursor(func):
    """
    Decorator that provides a cursor to the calling function

    Adds the cursor as the second argument to the calling functions

    Requires that the function being decorated is an instance of a class or object
    that yields a cursor from a get_cursor() coroutine or provides such an object
    as the first argument in its signature

    Yields:
        A client-side cursor
    """

    @wraps(func)
    def wrapper(cls, *args, **kwargs):
        with (yield from cls.get_cursor()) as c:
            return (yield from func(cls, c, *args, **kwargs))

    return wrapper


def nt_cursor(func):
    """
    Decorator that provides a namedtuple cursor to the calling function

    Adds the cursor as the second argument to the calling functions

    Requires that the function being decorated is an instance of a class or object
    that yields a cursor from a get_cursor(cursor_type=CursorType.NAMEDTUPLE) coroutine or provides such an object
    as the first argument in its signature

    Yields:
        A client-side namedtuple cursor
    """

    @wraps(func)
    def wrapper(cls, *args, **kwargs):
        with (yield from cls.get_cursor(_CursorType.NAMEDTUPLE)) as c:
            return (yield from func(cls, c, *args, **kwargs))

    return wrapper


def transaction(func):
    """
    Provides a transacted cursor which will run in autocommit=false mode

    For any exception the transaction will be rolled back.
    Requires that the function being decorated is an instance of a class or object
    that yields a cursor from a get_cursor(cursor_type=CursorType.NAMEDTUPLE) coroutine or provides such an object
    as the first argument in its signature

    Yields:
        A client-side transacted named cursor
    """

    @wraps(func)
    def wrapper(cls, *args, **kwargs):
        with (yield from cls.get_cursor(_CursorType.NAMEDTUPLE)) as c:
            try:
                yield from c.execute('BEGIN')
                result = (yield from func(cls, c, *args, **kwargs))
            except Exception as e:
                yield from c.execute('ROLLBACK')
                raise e
            else:
                yield from c.execute('COMMIT')
                return result

    return wrapper


class PostgresStoreV2:

    _insert_string = "insert into {} ({}) values ({}) returning *;"
    _bulk_insert_string = "insert into {} ({}) values"
    _update_string = "update {} set ({}) = ({}) where ({}) returning *;"
    _select_all_string_with_condition = "select * from {} where ({}) limit {} offset {};"
    _select_all_string_with_condition_group = "select * from {} where ({})  group by {} limit {} offset {};"
    _select_all_string = "select * from {} limit {} offset {};"
    _select_selective_column = "select {} from {} limit {} offset {};"
    _select_selective_column_group = "select {} from {}  group by {} limit {} offset {};"
    _select_selective_column_with_condition = "select {} from {} where ({}) limit {} offset {};"
    _select_selective_column_with_condition_group = "select {} from {} where ({}) group by {} limit {} offset {};"
    _select_all_string_with_condition_and_order_by = "select * from {} where ({}) order by {} limit {} offset {};"
    _select_all_string_with_condition_and_order_by_group = "select * from {} where ({}) group by {} order by {} limit {} offset {};"
    _select_all_string_with_order_by = "select * from {} order by {} limit {} offset {};"
    _select_selective_column_with_order_by = "select {} from {} order by {} limit {} offset {};"
    _select_selective_column_with_order_by_group = "select {} from {}  group by {} order by {} limit {} offset {};"
    _select_selective_column_with_condition_and_order_by = "select {} from {} where ({}) order by {} limit {} offset {};"
    _select_selective_column_with_condition_and_order_by_group = "select {} from {} where ({}) group by {} order by {} limit {} offset {};"
    _delete_query = "delete from {} where ({});"
    _count_query = "select count(*) from {};"
    _count_query_where = "select count(*) from {} where {};"
    _OR = ' or '
    _AND = ' and '
    _LPAREN = '('
    _RPAREN = ')'
    _WHERE_AND = '{} {} %s'
    _PLACEHOLDER = ' %s,'
    _COMMA = ', '
    _return_val = ' returning *;'

    def __init__(self, database: str, user: str, password: str, host: str, port: int, *, use_pool: bool = True,
                enable_ssl: bool = False, minsize=1, maxsize=10, keepalives_idle=5, keepalives_interval=4, echo=False,
                refresh_period=-1):

        self._use_pool = use_pool
        self._pool_pending = asyncio.Semaphore(1)

        self._connection_params = {}
        self._use_pool = use_pool
        self.refresh_period = refresh_period
        self._connection_params['database'] = database
        self._connection_params['user'] = user
        self._connection_params['password'] = password
        self._connection_params['host'] = host
        self._connection_params['port'] = port
        self._connection_params['sslmode'] = 'prefer' if enable_ssl else 'disable'
        self._connection_params['minsize'] = minsize
        self._connection_params['maxsize'] = maxsize
        self._connection_params['keepalives_idle'] = keepalives_idle
        self._connection_params['keepalives_interval'] = keepalives_interval
        self._connection_params['echo'] = echo

        # Initialize DB pool
        self._pool = None

        # Initialize periodic cleaning of connections
        asyncio.async(self._periodic_cleansing())

    @coroutine
    def _initialize_pool(self):
        """
        Yields: existing db connection pool
        """
        if len(self._connection_params) < 5:
            raise ConnectionError('Please call SQLStore.connect before calling this method')
        if not self._pool:
            with (yield from self._pool_pending):
                if not self._pool:
                    self._pool = yield from create_pool(**self._connection_params)

    @coroutine
    def initialize_pool(self):
        yield from self._initialize_pool()

    @coroutine
    def _periodic_cleansing(self):
        """
        Periodically cleanses idle connections in pool
        """
        if self.refresh_period > 0:
            yield from asyncio.sleep(self.refresh_period * 60)
            logging.getLogger().info("Clearing unused DB connections")
            yield from self._pool.clear()
            asyncio.async(self._periodic_cleansing())

    @coroutine
    def get_cursor(self, cursor_type=_CursorType.PLAIN) -> Cursor:
        """
        Yields:
            new client-side cursor from existing db connection pool
        """
        if self._use_pool:
            yield from self.initialize_pool()
            _connection_source = self._pool
        else:
            _connection_source = yield from aiopg.connect(echo=False, **self._connection_params)

        if cursor_type == _CursorType.PLAIN:
            _cur = yield from _connection_source.cursor()
        if cursor_type == _CursorType.NAMEDTUPLE:
            _cur = yield from _connection_source.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)
        if cursor_type == _CursorType.DICT:
            _cur = yield from _connection_source.cursor(cursor_factory=psycopg2.extras.DictCursor)

        if not self._use_pool:
            _cur = cursor_context_manager(_connection_source, _cur)

        return _cur

    @coroutine
    @cursor
    def count(self, cur, table: str, where_keys: list = None):
        """
        gives the number of records in the table
        Args:
            table: a string indicating the name of the table
            where_keys: where clause to be used
        Returns:
            an integer indicating the number of records in the table

        """

        if where_keys:
            where_clause, values = self._get_where_clause_with_values(where_keys)
            query = self._count_query_where.format(table, where_clause)
            q, t = query, values
        else:
            query = self._count_query.format(table)
            q, t = query, ()
        yield from cur.execute(q, t)
        result = yield from cur.fetchone()
        return int(result[0])

    @coroutine
    @nt_cursor
    def insert(self, cur, table: str, values: dict):
        """
        Creates an insert statement with only chosen fields

        Args:
            table: a string indicating the name of the table
            values: a dict of fields and values to be inserted

        Returns:
            A 'Record' object with table columns as properties

        """
        keys = self._COMMA.join(values.keys())
        value_place_holder = self._PLACEHOLDER * len(values)
        query = self._insert_string.format(table, keys, value_place_holder[:-1])
        yield from cur.execute(query, tuple(values.values()))
        return (yield from cur.fetchone())

    @coroutine
    @nt_cursor
    def bulk_insert(self, cur, table: str, records: list):
        """
        Creates an insert statement with only chosen fields for a set of entries

        Args:
        table: a string indicating the name of the table
        records: A list of dictionaries consisting of all the records to be inserted
        :Returns:
           A set 'Record' objects with table columns as properties
        """
        keys = self._COMMA.join(records[0].keys())
        value_ordered = list()
        for record in records:
            value_ordered.append([record[key] for key in records[0]])
        value_place_holder = self._LPAREN + (self._PLACEHOLDER * len(records[0]))[:-1] + self._RPAREN
        values = ','.join(
            (yield from cur.mogrify(value_place_holder, tuple(rec))).decode("utf-8") for rec in value_ordered)
        yield from cur.execute(self._bulk_insert_string.format(table, keys) + values + self._return_val)
        return (yield from cur.fetchall())

    @coroutine
    @nt_cursor
    def update(self, cur, table: str, values: dict, where_keys: list) -> tuple:
        """
        Creates an update query with only chosen fields
        Supports only a single field where clause

        Args:
            table: a string indicating the name of the table
            values: a dict of fields and values to be inserted
            where_keys: list of dictionary
            example of where keys: [{'name':('>', 'cip'),'url':('=', 'cip.com'},{'type':{'<=', 'manufacturer'}}]
            where_clause will look like ((name>%s and url=%s) or (type <= %s))
            items within each dictionary get 'AND'-ed and dictionaries themselves get 'OR'-ed

        Returns:
            an integer indicating count of rows deleted

        """
        keys = self._COMMA.join(values.keys())
        value_place_holder = self._PLACEHOLDER * len(values)
        where_clause, where_values = self._get_where_clause_with_values(where_keys)
        query = self._update_string.format(table, keys, value_place_holder[:-1], where_clause)
        yield from cur.execute(query, (tuple(values.values()) + where_values))
        return (yield from cur.fetchall())

    def _get_where_clause_with_values(self, where_keys):
        values = []
        def make_and_query(ele: dict):
            and_query = self._AND.join([self._WHERE_AND.format(e[0], e[1][0]) for e in ele.items()])
            values.extend([val[1] for val in ele.values()])
            return self._LPAREN + and_query + self._RPAREN

        return self._OR.join(map(make_and_query, where_keys)), tuple(values)

    @coroutine
    @cursor
    def delete(self, cur, table: str, where_keys: list):
        """
        Creates a delete query with where keys
        Supports multiple where clause with and or or both

        Args:
            table: a string indicating the name of the table
            where_keys: list of dictionary
            example of where keys: [{'name':('>', 'cip'),'url':('=', 'cip.com'},{'type':{'<=', 'manufacturer'}}]
            where_clause will look like ((name>%s and url=%s) or (type <= %s))
            items within each dictionary get 'AND'-ed and dictionaries themselves get 'OR'-ed

        Returns:
            an integer indicating count of rows deleted

        """
        where_clause, values = self._get_where_clause_with_values(where_keys)
        query = self._delete_query.format(table, where_clause)
        yield from cur.execute(query, values)
        return cur.rowcount

    @coroutine
    @nt_cursor
    def select(self, cur, table: str, order_by: str = None, columns: list = None, where_keys: list = None, limit=100,
               offset=0, group_by: str = None):
        """
        Creates a select query for selective columns with where keys
        Supports multiple where claus with and or or both

        Args:
            table: a string indicating the name of the table
            order_by: a string indicating column name to order the results on
            columns: list of columns to select from
            where_keys: list of dictionary
            limit: the limit on the number of results
            offset: offset on the results

            example of where keys: [{'name':('>', 'cip'),'url':('=', 'cip.com'},{'type':{'<=', 'manufacturer'}}]
            where_clause will look like ((name>%s and url=%s) or (type <= %s))
            items within each dictionary get 'AND'-ed and across dictionaries get 'OR'-ed

        Returns:
            A list of 'Record' object with table columns as properties

        """

        if columns:
            columns_string = self._COMMA.join(columns)
            if group_by:
                if where_keys:
                    where_clause, values = self._get_where_clause_with_values(where_keys)
                    if order_by:
                        query = self._select_selective_column_with_condition_and_order_by_group.format(columns_string,
                                                                                                      table,
                                                                                                      where_clause,
                                                                                                      group_by,
                                                                                                      order_by, limit,
                                                                                                      offset)
                    else:
                        query = self._select_selective_column_with_condition_group.format(columns_string, table,
                                                                                         where_clause,
                                                                                         group_by, limit, offset)
                    q, t = query, values
                else:
                    if order_by:
                        query = self._select_selective_column_with_order_by_group.format(columns_string, table, group_by,
                                                                                        order_by, limit, offset)
                    else:
                        query = self._select_selective_column_group.format(columns_string, table, group_by, limit,
                                                                          offset)
                    q, t = query, ()
            else:
                if where_keys:
                    where_clause, values = self._get_where_clause_with_values(where_keys)
                    if order_by:
                        query = self._select_selective_column_with_condition_and_order_by.format(columns_string, table,
                                                                                                where_clause,
                                                                                                order_by, limit, offset)
                    else:
                        query = self._select_selective_column_with_condition.format(columns_string, table, where_clause,
                                                                                   limit, offset)
                    q, t = query, values
                else:
                    if order_by:
                        query = self._select_selective_column_with_order_by.format(columns_string, table, order_by,
                                                                                  limit, offset)
                    else:
                        query = self._select_selective_column.format(columns_string, table, limit, offset)
                    q, t = query, ()
        else:
            if where_keys:
                where_clause, values = self._get_where_clause_with_values(where_keys)
                if order_by:
                    query = self._select_all_string_with_condition_and_order_by.format(table, where_clause, order_by,
                                                                                      limit, offset)
                else:
                    query = self._select_all_string_with_condition.format(table, where_clause, limit, offset)
                q, t = query, values
            else:
                if order_by:
                    query = self._select_all_string_with_order_by.format(table, order_by, limit, offset)
                else:
                    query = self._select_all_string.format(table, limit, offset)
                q, t = query, ()
        yield from cur.execute(q, t)
        return (yield from cur.fetchall())

    @classmethod
    @coroutine
    @nt_cursor
    def call_stored_procedure(cls, cur, procname, parameters=None, timeout=None):
        yield from cur.callproc(procname, parameters=parameters, timeout=timeout)
        return (yield from cur.fetchall())

    @coroutine
    @cursor
    def mogrify(self, cur, query, parameters=None):
        return cur.mogrify(query, parameters=parameters)

    @coroutine
    @nt_cursor
    def raw_sql(self, cur, query: str, values: tuple):
        """
        Run a raw sql query

        Args:
            query : query string to execute
            values : tuple of values to be used with the query

        Returns:
            result of query as list of named tuple

        """
        yield from cur.execute(query, values)
        return (yield from cur.fetchall())


@contextmanager
def cursor_context_manager(conn, cur):
    try:
        yield cur
    finally:
        cur._impl.close()
        conn.close()


if __name__ == '__main__':
    import time
    postgres_manager = PostgresStore('wallet_db', 'wallet_user', 'wallet_pass', 'postgres.1mginfra.com', 5432,
                                     use_pool=True)
    print(postgres_manager)
    time.sleep(100)