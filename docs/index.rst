.. _index:

#################
CrateDB Reference
#################


*****
About
*****

CrateDB is a distributed and scalable open-source SQL database for storing and
analyzing massive amounts of data in near real-time, even with complex queries.
It is PostgreSQL-compatible, and based on Lucene.

Users are operating CrateDB clusters that store information in the range of
billions of records, and terabytes of data, equally accessible without any
retrieval penalty on data point age.


*****
Usage
*****

Learn about the concepts of CrateDB, general usage information, its client
interfaces / connectivity options, and its SQL syntax, including special
dialect features.

.. grid:: 1 2 2 2
    :margin: 4 4 0 0
    :gutter: 3

    .. grid-item-card:: :material-outlined:`library_books;2em` Handbook
        :link: general/index
        :link-type: doc
        :link-alt: Handbook and general use

        Feature overview / handbook.
        Primarily of interest to general users.

    .. grid-item-card:: :octicon:`code;2em` SQL syntax
        :link: sql/index
        :link-type: doc
        :link-alt: SQL syntax reference

        Complete SQL syntax reference.
        Learn about all the dialect options of CrateDB.

    .. grid-item-card:: :material-outlined:`lightbulb;2em` Concepts
        :link: concepts/index
        :link-type: doc
        :link-alt: Concepts

        Important concepts of CrateDB.
        Explains the architecture of joins, clustering, storage
        consistency, and -resiliency.

    .. grid-item-card:: :material-outlined:`link;2em` Connectivity
        :link: interfaces/index
        :link-type: doc
        :link-alt: Connectivity options

        The two primary client interfaces.
        HTTP and PostgreSQL.


******
Manage
******

Learn about how to configure and manage CrateDB in day-to-day operations.

.. grid:: 1 2 2 3
    :margin: 4 4 0 0
    :gutter: 3


    .. grid-item-card:: :material-outlined:`settings;2em` Configuration
        :link: config/index
        :link-type: doc
        :link-alt: Configuration

        How to configure CrateDB.

    .. grid-item-card:: :material-outlined:`admin_panel_settings;2em` Administration
        :link: admin/index
        :link-type: doc
        :link-alt: Administration

        Feature overview primarily of interest to system administrators.

    .. grid-item-card:: :material-outlined:`read_more;2em` Appendices
        :link: appendices/index
        :link-type: doc
        :link-alt: Appendices

        Supplementary information for the CrateDB reference manual.


    .. grid-item-card:: :material-outlined:`text_snippet;2em` Release notes
        :link: release_notes
        :link-type: ref
        :link-alt: Release notes
        :columns: 12

        Information about individual CrateDB releases, including upgrade and changelog information.


*******
Connect
*******

.. vale off

This section introduces you to the canonical set of database drivers, client-
and developer-applications, and how to configure them to connect to CrateDB.
Just to name a few, it is about the CrateDB Admin UI, ``crash``, ``psql``,
DataGrip, and DBeaver applications, the Java/JDBC/Python drivers, the SQLAlchemy
and Flink dialects, and more.

.. vale on

CrateDB integrates well with a diverse set of applications and tools concerned
with analytics, visualization, and data wrangling, in the areas of ETL, BI,
metrics aggregation and monitoring, and more.

.. grid:: 1 2 2 2
    :margin: 4 4 0 0
    :gutter: 3

    .. grid-item-card:: :material-outlined:`table_chart;2em` Admin UI
        :link: crate-admin-ui:index
        :link-type: ref
        :link-alt: Admin UI

        CrateDB ships with an integrated Admin UI, which can be used to explore
        data, schema metadata, and cluster status information.

    .. grid-item-card:: :material-outlined:`integration_instructions;2em` Clients and Integrations
        :link: crate-clients-tools:index
        :link-type: ref
        :link-alt: Clients and Integrations

        Learn about compatible client applications and tools, and how to configure
        your favorite client library to connect to a CrateDB cluster.



.. NOTE::

    This section of the documentation assumes you know the basics. If not, please check
    out the `Tutorials`_ section for corresponding material to get started.

.. SEEALSO::

    CrateDB is an open source project and is `hosted on GitHub`_.

.. rubric:: Table of contents

.. toctree::
   :maxdepth: 2

   concepts/index
   cli-tools
   config/index
   general/index
   admin/index
   sql/index
   interfaces/index
   appendices/index


.. _Tutorials: https://crate.io/docs/crate/tutorials/en/latest/
.. _hosted on GitHub: https://github.com/crate/crate
