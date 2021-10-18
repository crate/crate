.. highlight:: psql

.. _sql-alter-publication:

=====================
``ALTER PUBLICATION``
=====================

You can use the ``ALTER PUBLICATION`` :ref:`statement <gloss-statement>` to
update the list of published tables on the current cluster.

.. SEEALSO::

    :ref:`CREATE PUBLICATION <sql-create-publication>`
    :ref:`DROP PUBLICATION <sql-drop-publication>`

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


.. _sql-alter-publication-synopsis:

Synopsis
========

::

    ALTER PUBLICATION name ADD TABLE table_name [, ...]
    ALTER PUBLICATION name SET TABLE table_name [, ...]
    ALTER PUBLICATION name DROP TABLE table_name [, ...]

.. _sql-alter-publication-desc:

Description
===========

Update the list of published table according to the command.


Parameters
==========

**name**
  The name of the publication to be updated.

**ADD TABLE**
  Add one or more tables into the list of existing publications.

**DROP TABLE**
   Remove one or more tables from the list of existing publications.

**SET TABLE**
    Replace the list of existing publications with the new one.