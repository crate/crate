========================
Crate Data Java Client
========================

.. highlight:: java

The client module exposes a very simple interface to query Crate using
SQL.

Installation
============

Right now the client must be built from source, as of now, no pre-built
JAR packages are available via Maven.

See :ref:`Development <client_develop>` for further details.


Usage
=====

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

.. note::

   Queries are executed asynchronous. `client.sql("")` will return a
   `Future<SQLResponse>` and code execution is only blocked if
   `.actionGet()` is called on it.
