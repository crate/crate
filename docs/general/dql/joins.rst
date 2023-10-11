.. highlight:: psql

.. _sql_joins:

Joins
=====

When selecting data from CrateDB, you can :ref:`join
<sql-select-joined-relation>` one or more relations (e.g., tables) to combine
columns into one result set.

.. SEEALSO::

    `Join (SQL)`_

.. rubric:: Table of contents

.. contents::
   :local:


.. _cross-joins:

Cross joins
-----------

Referencing two tables results in a ``CROSS JOIN``.

The result is computed by creating every possible combination (i.e., a
`cartesian product`_) of their rows (``t1 * t2 * t3 * tn``) and then applying
the given query operation on it (e.g., ``WHERE`` clause, ``SELECT`` list,
``ORDER BY`` clause, and so on)::

    cr> select articles.name as article, colors.name as color, price
    ... from articles cross join colors
    ... where price > 5000.0
    ... order by price, color, article;
    +------------------------------+---------------+----------+
    | article                      | color         |    price |
    +------------------------------+---------------+----------+
    | Infinite Improbability Drive | Antique White | 19999.99 |
    | Infinite Improbability Drive | Gold          | 19999.99 |
    | Infinite Improbability Drive | Midnight Blue | 19999.99 |
    | Infinite Improbability Drive | Olive Drab    | 19999.99 |
    | Starship Titanic             | Antique White | 50000.0  |
    | Starship Titanic             | Gold          | 50000.0  |
    | Starship Titanic             | Midnight Blue | 50000.0  |
    | Starship Titanic             | Olive Drab    | 50000.0  |
    +------------------------------+---------------+----------+
    SELECT 8 rows in set (... sec)

Cross joins can be done explicitly using the ``CROSS JOIN`` statement as shown
in the example above, or implicitly by just specifying two or more tables in
the ``FROM`` list::

    cr> select articles.name as article, colors.name as color, price
    ... from articles, colors
    ... where price > 5000.0
    ... order by price, color, article;
    +------------------------------+---------------+----------+
    | article                      | color         |    price |
    +------------------------------+---------------+----------+
    | Infinite Improbability Drive | Antique White | 19999.99 |
    | Infinite Improbability Drive | Gold          | 19999.99 |
    | Infinite Improbability Drive | Midnight Blue | 19999.99 |
    | Infinite Improbability Drive | Olive Drab    | 19999.99 |
    | Starship Titanic             | Antique White | 50000.0  |
    | Starship Titanic             | Gold          | 50000.0  |
    | Starship Titanic             | Midnight Blue | 50000.0  |
    | Starship Titanic             | Olive Drab    | 50000.0  |
    +------------------------------+---------------+----------+
    SELECT 8 rows in set (... sec)


.. _inner-joins:

Inner joins
-----------

Inner joins require each record of one table to have matching records on the
other table::

    cr> select s.id, s.table_name, t.number_of_shards
    ... from sys.shards s, information_schema.tables t
    ... where s.table_name = t.table_name
    ... and s.table_name = 'employees'
    ... order by s.id;
    +----+------------+------------------+
    | id | table_name | number_of_shards |
    +----+------------+------------------+
    |  0 | employees  |                4 |
    |  1 | employees  |                4 |
    |  2 | employees  |                4 |
    |  3 | employees  |                4 |
    +----+------------+------------------+
    SELECT 4 rows in set (... sec)


.. _outer-joins:

Outer joins
-----------


Left outer joins
................

Left outer join returns tuples for all matching records of the left-hand and
right-hand relation like :ref:`inner joins <inner-joins>`. Additionally it
returns tuples for all other records from left-hand that don't match any record
on the right-hand by using ``NULL`` values for the columns of the right-hand
relation::

    cr> select e.name || ' ' || e.surname as employee, coalesce(d.name, '') as manager_of_department
    ... from employees e left join departments d
    ... on e.id = d.manager_id
    ... order by e.id;
    +--------------------+-----------------------+
    | employee           | manager_of_department |
    +--------------------+-----------------------+
    | John Doe           | Administration        |
    | John Smith         | IT                    |
    | Sean Lee           |                       |
    | Rebecca Sean       |                       |
    | Tim Ducan          |                       |
    | Robert Duval       |                       |
    | Clint Johnson      |                       |
    | Sarrah Mcmillan    |                       |
    | David Limb         |                       |
    | David Bowe         |                       |
    | Smith Clark        | Marketing             |
    | Ted Kennedy        |                       |
    | Ronald Reagan      |                       |
    | Franklin Rossevelt |                       |
    | Sam Malone         |                       |
    | Marry Georgia      |                       |
    | Tim Doe            | Human Resources       |
    | Tim Malone         | Purchasing            |
    +--------------------+-----------------------+
    SELECT 18 rows in set (... sec)


Right outer joins
.................

Right outer join returns tuples for all matching records of the right-hand and
left-hand relation like :ref:`inner joins <inner-joins>`. Additionally it
returns tuples for all other records from right-hand that don't match any
record on the left-hand by using ``NULL`` values for the columns of the
left-hand relation::

    cr> select e.name || ' ' || e.surname as employee, d.name as manager_of_department
    ... from employees e right join departments d
    ... on e.id = d.manager_id
    ... order by d.id;
    +-------------+-----------------------+
    | employee    | manager_of_department |
    +-------------+-----------------------+
    | John Doe    | Administration        |
    | Smith Clark | Marketing             |
    | Tim Malone  | Purchasing            |
    | Tim Doe     | Human Resources       |
    |             | Shipping              |
    | John Smith  | IT                    |
    +-------------+-----------------------+
    SELECT 6 rows in set (... sec)


Full outer joins
................

Full outer join returns tuples for all matching records of the left-hand and
right-hand relation like :ref:`inner joins <inner-joins>`. Additionally it
returns tuples for all other records from left-hand that don't match any record
on the right-hand by using ``NULL`` values for the columns of the right-hand
relation. Additionally it returns tuples for all other records from right-hand
that don't match any record on the left-hand by using ``NULL`` values for the
columns of the left-hand relation::

    cr> select e.name || ' ' || e.surname as employee, coalesce(d.name, '') as manager_of_department
    ... from employees e full join departments d
    ... on e.id = d.manager_id
    ... order by e.id;
    +--------------------+-----------------------+
    | employee           | manager_of_department |
    +--------------------+-----------------------+
    | John Doe           | Administration        |
    | John Smith         | IT                    |
    | Sean Lee           |                       |
    | Rebecca Sean       |                       |
    | Tim Ducan          |                       |
    | Robert Duval       |                       |
    | Clint Johnson      |                       |
    | Sarrah Mcmillan    |                       |
    | David Limb         |                       |
    | David Bowe         |                       |
    | Smith Clark        | Marketing             |
    | Ted Kennedy        |                       |
    | Ronald Reagan      |                       |
    | Franklin Rossevelt |                       |
    | Sam Malone         |                       |
    | Marry Georgia      |                       |
    | Tim Doe            | Human Resources       |
    | Tim Malone         | Purchasing            |
    |                    | Shipping              |
    +--------------------+-----------------------+
    SELECT 19 rows in set (... sec)


Join conditions
---------------

CrateDB supports all :ref:`operators <gloss-operator>` and :ref:`scalar
functions <scalar-functions>` as join conditions in the ``WHERE`` clause.

Example with ``within`` scalar function::

    cr> select photos.name, countries.name
    ... from countries, photos
    ... where within(location, geo)
    ... order by countries.name, photos.name;
    +--------------+---------+
    | name         | name    |
    +--------------+---------+
    | Eiffel Tower | France  |
    | Berlin Wall  | Germany |
    +--------------+---------+
    SELECT 2 rows in set (... sec)


.. _available-join-algo:

Available join algorithms
-------------------------


Nested loop join algorithm
..........................

The nested loop algorithm :ref:`evaluates <gloss-evaluation>` the join
conditions on every record of the left-hand table with every record of the
right-hand table in a distributed manner (for each shard of the used
tables). The right-hand table is scanned once for every row in the left-hand
table.

This is the default algorithm used for all types of joins.


Block hash join algorithm
.........................

The performance of `equi-joins`_ is substantially improved by using the `hash
join`_ algorithm. At first, one relation is scanned and loaded into a hash
table using the attributes of the join conditions as hash keys. Once the hash
table is built, the second relation is scanned and the join condition values of
every row are hashed and matched against the hash table.

In order to built a hash table even if the first relation wouldn't fit into the
available memory, only a certain block size of a relation is loaded at
once. The whole operation will be repeated with the next block of the first
relation once scanning the second relation has finished.

This optimisation cannot be applied unless the join is an ``INNER`` join and
the join condition satisfies the following rules:

- Contains at least one ``EQUAL`` :ref:`operator <gloss-operator>`

- Contains no ``OR`` operator

- Every argument of a ``EQUAL`` operator can only references fields from one
  relation

The `hash join`_ algorithm is faster but has a bigger memory footprint. As such
it can explicitly be disabled on demand when memory is scarce using the session
setting :ref:`enable_hashjoin <conf-session-enable-hashjoin>`::


  SET enable_hashjoin=false

Limitations
-----------

Joining more than 2 tables can result in poor execution plans.

Internally the relations are joined in pairs. So for example in a 3 table join,
we'd join `(r1 ⋈ r2) ⋈ r3` (r1 with r2 first, then with r3). The poor execution
plan could happen as there is no optimization in place which improves the join
ordering. Ideally we'd join those relations first which narrow the intermediate
result set to a large degree, so that later joins have less work to do. In the
example before, joining `r1 ⋈ (r2 ⋈ r3)` might be the better order.


.. _Cartesian Product: https://en.wikipedia.org/wiki/Cartesian_product
.. _Equi-Joins: https://en.wikipedia.org/wiki/Join_(SQL)#Equi-join
.. _Hash Join: https://en.wikipedia.org/wiki/Hash_join
.. _Join (SQL): https://en.wikipedia.org/wiki/Join_(SQL)
