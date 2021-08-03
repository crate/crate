.. highlight:: psql

.. _sql_dql_geo_search:

==========
Geo search
==========

.. rubric:: Table of contents

.. contents::
   :local:


Introduction
============

CrateDB can be used to store and query geographical information of many kinds
using the :ref:`data-types-geo-point` and :ref:`data-types-geo-shape` types. With
these it is possible to store geographical locations, ways, shapes, areas and
other entities. These can be queried for distance, containment, intersection
and so on, making it possible to create apps and services with rich
geographical features.

:ref:`Geographic shapes <data-types-geo-shape>` are stored using special
indices. :ref:`Geographic points <data-types-geo-point>` are represented by
their coordinates. They are represented as columns of the respective datatypes.

Geographic indices for :ref:`geo_shape <data-types-geo-shape>` columns are used
in order to speed up geographic searches even on complex shapes. This indexing
process results in a representation that is not exact (See :ref:`geo_shape
<data-types-geo-shape>` for details). CrateDB does not operate on vector shapes
but on a kind of a grid with the given precision as resolution.

Creating tables containing geographic information is straightforward::

    cr> CREATE TABLE country (
    ...   name text,
    ...   country_code text primary key,
    ...   shape geo_shape INDEX USING "geohash" WITH (precision='100m'),
    ...   capital text,
    ...   capital_location geo_point
    ... ) WITH (number_of_replicas=0);
    CREATE OK, 1 row affected  (... sec)

This table will contain the shape of a country and the location of its capital
alongside with other metadata. The shape is indexed with a maximum precision of
100 meters using a ``geohash`` index (For more information, see
:ref:`type-geo_shape-index`).

Let's insert Austria::

    cr> INSERT INTO country (name, country_code, shape, capital, capital_location)
    ... VALUES (
    ...  'Austria',
    ...  'at',
    ...  {type='Polygon', coordinates=[
    ...        [[16.979667, 48.123497], [16.903754, 47.714866],
    ...        [16.340584, 47.712902], [16.534268, 47.496171],
    ...        [16.202298, 46.852386], [16.011664, 46.683611],
    ...        [15.137092, 46.658703], [14.632472, 46.431817],
    ...        [13.806475, 46.509306], [12.376485, 46.767559],
    ...        [12.153088, 47.115393], [11.164828, 46.941579],
    ...        [11.048556, 46.751359], [10.442701, 46.893546],
    ...        [9.932448, 46.920728], [9.47997, 47.10281],
    ...        [9.632932, 47.347601], [9.594226, 47.525058],
    ...        [9.896068, 47.580197], [10.402084, 47.302488],
    ...        [10.544504, 47.566399], [11.426414, 47.523766],
    ...        [12.141357, 47.703083], [12.62076, 47.672388],
    ...        [12.932627, 47.467646], [13.025851, 47.637584],
    ...        [12.884103, 48.289146], [13.243357, 48.416115],
    ...        [13.595946, 48.877172], [14.338898, 48.555305],
    ...        [14.901447, 48.964402], [15.253416, 49.039074],
    ...        [16.029647, 48.733899], [16.499283, 48.785808],
    ...        [16.960288, 48.596982], [16.879983, 48.470013],
    ...        [16.979667, 48.123497]]
    ...  ]},
    ...  'Vienna',
    ...  [16.372778, 48.209206]
    ... );
    INSERT OK, 1 row affected (... sec)

.. Hidden: refresh country

   cr> REFRESH TABLE country;
   REFRESH OK, 1 row affected  (... sec)

.. CAUTION::

   Geoshapes has to be fully valid by `ISO 19107`_. If you have problems
   importing geo data, they may not be fully valid. In most cases they could be
   repaired using this tool: https://github.com/tudelft3d/prepair

.. NOTE::

   When using a polygon shape that resembles a rectangle, and that rectangle is
   wider than 180 degrees, the CrateDB geoshape validator will convert it into
   a multipolygon consisting of 2 rectangular shapes covering the narrower area
   between the 4 original points split by the dateline (+/- 180deg).

   This is due to CrateDB operating in the geospatial context of the earth.

.. Hidden: refresh countries

   cr> REFRESH TABLE countries;
   REFRESH OK, 1 row affected  (... sec)

:ref:`Geographic points <data-types-geo-point>` can be inserted as a ``double
precision`` array with longitude and latitude values as seen above or by using
a `WKT`_ string.

:ref:`Geographic shapes <data-types-geo-shape>` can be inserted as `GeoJSON`_
:ref:`object literal <type-geo_shape-literals>` or parameter as seen above
and as `WKT`_ string.

When it comes to get some meaningful insights into your geographical data
CrateDB supports different kinds of geographic queries.

Fast queries that leverage the geographic index are done using the
:ref:`sql_dql_geo_match`:


.. _sql_dql_geo_match:

``MATCH`` predicate
===================

The ``MATCH`` predicate can be used to perform multiple kinds of searches on
indices or indexed columns. While it can be used to perform :ref:`fulltext
searches <sql_dql_fulltext_search>` on analyzed indices of type
:ref:`type-text`, it is also handy for operating on geographic indices,
querying for relations between geographical shapes and points.

::

     MATCH (column_ident, query_term) [ using match_type ]

The ``MATCH`` predicate for geographical search supports a single
``column_ident`` of a ``geo_shape`` indexed column as first argument.

The second argument, the ``query_term`` is taken to match against the indexed
``geo_shape``.

The matching operation is determined by the ``match_type`` which determines the
spatial relation we want to match. Available ``match_types`` are:

:intersects:
  (Default) If the two shapes share some points and/or area, they are
  intersecting and considered matching using this ``match_type``. This also
  precludes containment or complete equality.

:disjoint:
  If the two shapes share no single point or area, they are disjoint. This is
  the opposite of ``intersects``.

:within:
  If the indexed ``column_ident`` shape is completely inside the ``query_term``
  shape, they are considered matching using this ``match_type``.

.. NOTE::

   The ``MATCH`` predicate can only be used in the :ref:`sql_dql_where_clause`
   and on user-created tables. Using the ``MATCH`` predicate on system tables
   is not supported.

   One ``MATCH`` predicate cannot combine columns of both relations of a join.

   Additionally, ``MATCH`` predicates cannot be used on columns of both
   relations of a join if they cannot be logically applied to each of them
   separately. For example:

   This is allowed::

        FROM t1, t2
       WHERE match(t1.shape, 'POINT(1.1 2.2)')
         AND match(t2.shape, 'POINT(3.3 4.4)')

   But this is not::

        FROM t1, t2
       WHERE match(t1.shape, 'POINT(1.1 2.2)')
          OR match(t2.shape, 'POINT(3.3 4.4)')``

Having a table ``countries`` with a ``GEO_SHAPE`` column ``geo``, indexed using
``geohash``, you can query that column using the ``MATCH`` predicate with
different match types as described above::

    cr> SELECT name from countries
    ... WHERE match("geo",
    ...   'LINESTRING (13.3813 52.5229, 11.1840 51.5497, 8.6132 50.0782, 8.3715 47.9457, 8.5034 47.3685)'
    ... );
    +---------+
    | name    |
    +---------+
    | Germany |
    +---------+
    SELECT 1 row in set (... sec)

::

    cr> SELECT name from countries
    ... WHERE match("geo",
    ...   'LINESTRING (13.3813 52.5229, 11.1840 51.5497, 8.6132 50.0782, 8.3715 47.9457, 8.5034 47.3685)'
    ... ) USING disjoint
    ... ORDER BY name;
    +--------------+
    | name         |
    +--------------+
    | Austria      |
    | France       |
    | South Africa |
    | Turkey       |
    +--------------+
    SELECT 4 rows in set (... sec)


Exact queries
=============

*Exact* queries are done using the following :ref:`scalar functions
<scalar-functions>`:

 * :ref:`scalar-intersects`

 * :ref:`scalar-within`

 * :ref:`scalar-distance`

They are exact, but this comes at the price of performance.

They do not make use of the index but work on the `GeoJSON`_ that was inserted
to compute the shape vector. This access is quite expensive and may
significantly slow down your queries.

For fast querying, use the :ref:`sql_dql_geo_match`.

But executed on a limited result set, they will help you get precise insights
into your geographic data::

    cr> SELECT within(capital_location, shape) AS capital_in_country
    ... FROM country;
    +--------------------+
    | capital_in_country |
    +--------------------+
    | TRUE               |
    +--------------------+
    SELECT 1 row in set (... sec)

::

    cr> SELECT distance(capital_location, 'POINT(0.0 90.0)') as from_northpole
    ... FROM country ORDER BY country_code;
    +-------------------+
    |    from_northpole |
    +-------------------+
    | 4646930.675034644 |
    +-------------------+
    SELECT 1 row in set (... sec)

::

    cr> SELECT intersects(
    ...   {type='LineString', coordinates=[[13.3813, 52.5229],
    ...                                    [11.1840, 51.5497],
    ...                                    [8.6132,  50.0782],
    ...                                    [8.3715,  47.9457],
    ...                                    [8.5034,  47.3685]]},
    ...   shape) as berlin_zurich_intersects
    ... FROM country ORDER BY country_code;
    +--------------------------+
    | berlin_zurich_intersects |
    +--------------------------+
    | FALSE                    |
    +--------------------------+
    SELECT 1 row in set (... sec)

.. Hidden: drop the country table

    cr> DROP TABLE country;
    DROP OK, 1 row affected  (... sec)

Nonetheless these :ref:`scalars <gloss-scalar>` can be used everywhere in a SQL
query where scalar functions are allowed.


.. _GeoJSON: https://geojson.org/
.. _WKT: https://en.wikipedia.org/wiki/Well-known_text
.. _ISO 19107: https://www.iso.org/iso/catalogue_detail.htm?csnumber=26012
