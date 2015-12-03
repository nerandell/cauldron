Cauldron
========

Cauldron is an asyncio based library that removes boilerplate code when using databases.
Currently it supports using postgresql and redis.

Requirements
------------
- Python >= 3.4.3
- asyncio_ 

.. _asyncio: https://pypi.python.org/pypi/asyncio


Installation
------------

To install via pip:

.. code-block:: bash

    $ pip install cauldron

To install from source:

.. code-block:: bash

    $ git clone https://github.com/nerandell/cauldron
    $ cd cauldron
    $ python setup.py install

Usage
-----

Cauldron currently supports postgres and redis. It uses aiopg_ and aioredis_ internally but removes a lot of
boilerplate code that is usually written.

.. _aiopg: https://github.com/aio-libs/aiopg
.. _aioredis: https://github.com/aio-libs/aioredis

Sample code using aiopg:

.. code-block:: python

    import asyncio
    from aiopg.pool import create_pool

    dsn = 'dbname=jetty user=nick password=1234 host=localhost port=5432'


    class UsePostgres():

        @classmethod
        def test_select(cls):
            pool = yield from create_pool(dsn)

            with (yield from pool) as conn:
                cur = yield from conn.cursor()
                yield from cur.execute('SELECT 1')
                ret = yield from cur.fetchone()
                assert ret == (1,), ret


Using Cauldron:

.. code-block:: python

    from cauldron import PostgresStore

    class UseCauldron(PostgresStore):
        @classmethod
        def test_select(cls):
            rows = yield from cls.raw_query('select 1')
            print(rows)

Other Examples
^^^^^^^^^^^^^^

``cauldron`` also supports using different cursors in a way that you have to write minimal code.

Using default cursor
********************

.. code-block:: python

    from cauldron import PostgresStore

    class UseCauldron(PostgresStore):
        @classmethod
        @cursor
        def test_select(cls, cur):
            rows = yield from cls.raw_sql('select * from users')
            print(rows)

Using namedtuple_ cursor

.. code-block:: python

    from cauldron import PostgresStore

    class UseCauldron(PostgresStore):
        @classmethod
        @nt_cursor
        def test_select(cls, cur):
            rows = yield from cls.raw_sql('select * from users')
            print(rows)
            
.. _namedtuple: https://docs.python.org/3/library/collections.html#collections.namedtuple

Using dict cursor:

.. code-block:: python

    from cauldron import PostgresStore

    class UseCauldron(PostgresStore):
        @classmethod
        @dict_cursor
        def test_select(cls, cur):
            rows = yield from cls.raw_sql('select * from users')
            print(rows)

``cauldron`` also provides functionalities for common DB operations to make your code more readable

Inserting into db:

.. code-block:: python

    from cauldron import PostgresStore

    class UseCauldron(PostgresStore):
        @classmethod
        def store_user(cls, username, password):
            insert_dict = {'username': username, 'password': password}
            yield from cls.insert('user_table', insert_dict)

License
-------
``cauldron`` is offered under the MIT license.

Source code
-----------
The latest developer version is available in a github repository:
https://github.com/nerandell/cauldron
