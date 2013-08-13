============================
Crate SQL Plugin Development
============================

This file contains development specific information.


Package
=======

In pom.xml update the <version> tag of the package.

Building a jar file is done by maven with the command::

    >>> mvn clean package

The resulting jar file will reside in the folder ``releases``.


Testing
=======

To run all tests run::

    >>> mvn test

To run a specific test class::

    >>> mvn -Dtest=<className> test

The doc tests are specified in the ``DocumentationTest.java`` file.
Therefore, to run just the doc tests run::

    >>> mvn -Dtest=DocumentationTest test

The integration tests are specified in the ``IntegrationTest.java`` file.
Therefore, to run just the integration tests run::

    >>> mvn -Dtest=IntegrationTest test

Sample data
===========

The used mapping for the test data can be found under:

    ``src/main/resources/essetup/mappings/test_a.json``


The sample data for running the tests can be found under: 

    ``src/main/resources/essetup/data/test_a.json``
