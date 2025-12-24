.. highlight:: psql

.. _sql_supported_features:

=======================
SQL standard compliance
=======================

This page documents the standard SQL (`ISO/IEC 9075`_) features that
CrateDB supports, along with implementation notes and any associated caveats.

.. CAUTION::

    This list is approximate and features that are listed as supported might be
    nonconforming in their implementation. However, the main reference
    documentation always contains the most accurate information about
    the features CrateDB supports and how to use them.

.. SEEALSO::

    :ref:`SQL compatibility <appendix-compatibility>`

.. csv-filter::
   :header: ID,Package,#,Description,Supported,Verified,Comments
   :widths: 80,140,15,250,130
   :delim: U+0009
   :file: ../../server/src/main/resources/sql_features.tsv
   :exclude: {4: '(?i)N\\w*'}
   :included_cols: 0,1,2,3,6

.. _ISO/IEC 9075: https://www.iso.org/obp/ui/#iso:std:iso-iec:9075:-2:ed-4:v1:en
