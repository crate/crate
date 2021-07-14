.. highlight:: psql

.. _sql_supported_features:

=======================
SQL standard compliance
=======================

This page documents the standard SQL (`ISO/IEC 9075`_) features that
CrateDB supports, along with implementation notes and any associated caveats.

.. CAUTION::

    This list may be incomplete or lacking information. Consult the primary
    :ref:`SQL syntax <sql>` reference for the most complete and up-to-date
    information.

.. SEEALSO::

    :ref:`SQL compatibility <crate_standard_sql>`

.. csv-filter::
   :header: ID,Package,#,Description,Supported,Verified,Comments
   :widths: 80,140,15,250,130
   :delim: U+0009
   :file: ../../server/src/main/resources/sql_features.tsv
   :exclude: {4: '(?i)N\w*'}
   :included_cols: 0,1,2,3,6

.. _ISO/IEC 9075: https://www.iso.org/obp/ui/#iso:std:iso-iec:9075:-2:ed-4:v1:en
