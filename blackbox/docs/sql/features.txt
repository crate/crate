.. highlight:: psql
.. _sql_supported_features:

==================
Supported Features
==================

This section provides a list of features that CrateDB supports and to what
extent it conforms to the current SQL standard `ISO/IEC 9075`_ "Database
Language SQL".

This list is approximate and features that are listed as supported might be
nonconforming in their implementation. However, the main reference
documentation of CrateDB always contains the most accurate information about
what CrateDB supports, what they are and how to use them.

.. csv-table::
   :header: ID,Package,#,Description,Supported,Verified,Comments
   :widths: 80,140,15,250,130
   :delim: U+0009
   :file: ../../../sql/src/main/resources/sql_features.tsv
   :exclude: {4: '(?i)N\w*'}
   :included_cols: 0,1,2,3,6

.. _ISO/IEC 9075: https://www.iso.org/obp/ui/#iso:std:iso-iec:9075:-2:ed-4:v1:en
