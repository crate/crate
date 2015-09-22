.. image:: https://cdn.crate.io/web/2.0/img/crate-avatar_100x100.png
    :width: 100px
    :height: 100px
    :alt: Crate
    :target: https://crate.io

=================
Crate Java Client
=================

**For a more detailed documentation how to set up and use the Crate client
please refer to the client documentation on `https://crate.io/docs/`_.**

.. highlight:: xml

The client module exposes a very simple interface to query Crate using SQL.

Installation
============

Maven
-----

The easiest way to use the crate client is to include it as a dependency using
Maven::

    <dependency>
        <groupId>io.crate</groupId>
        <artifactId>crate-client</artifactId>
        <version>...</version>
    </dependency>

You can find the latest version of the client on Bintray_.

From Source
-----------

See :ref:`Development <client_develop>` for further details.

Usage
=====

.. highlight:: java

A minimal example is just a few lines of code::

    import io.crate.client.CrateClient;

    CrateClient client = new CrateClient("server1.crate.org:4300", "server2.crate.org:4300");
    SQLResponse r = client.sql("select firstName, lastName from users").actionGet();

    System.out.println(Arrays.toString(r.cols()));
    // outputs ["firstName", "lastName"]

    for (Object[] row: r.rows()){
        System.out.println(Arrays.toString(row));
    }
    // outputs the users. For example:
    // ["Arthur", "Dent"]
    // ["Ford", "Perfect"]

The `CrateClient` takes multiple servers as arguments. They are used in a
round-robin fashion to distribute the load. In case a server is unavailable it
will be skipped.

By default, the column data types are not serialized. In order to get
these, one must defined it at the `SQLRequest` object::

    import io.crate.client.CrateClient;

    CrateClient client = new CrateClient("server1.crate.org:4300", "server2.crate.org:4300");

    SQLRequest request = new SQLRequest("select firstName, lastName from users");
    request.includeTypesOnResponse(true);

    SQLResponse r = client.sql().actionGet();

    // Get the data type of the first column
    DataType dataType = r.columnTypes()[0]
    System.out.print(dataType.getName())
    // outputs: "string"

.. note::

   Queries are executed asynchronous. `client.sql("")` will return a
   `Future<SQLResponse>` and code execution is only blocked if
   `.actionGet()` is called on it.


.. _`https://crate.io/docs/`: https://crate.io/docs/projects/crate-java/stable/
.. _Bintray: https://bintray.com/crate/crate/crate-client/view

Logging
=======

Crate client relies on Elastic Search ESLogger facility which itself defaults to `log4j` framework.

Using Slf4j
-----------

In order to configure the crate client to use Slf4j, you need to configure the ESLoggerFactory to use Slf4j by using the following command (typically in your entry point)::

    ESLoggerFactory.setDefaultFactory(new Slf4jESLoggerFactory());
