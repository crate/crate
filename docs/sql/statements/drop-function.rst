.. highlight:: psql

.. _ref-drop-function:

=================
``DROP FUNCTION``
=================

Drop a :ref:`function <user-defined-functions>`.

Synopsis
========

::

    DROP FUNCTION [ IF EXISTS ] function_name
        ( [ [ argument_name ] argument_type [, ...] ] )


Description
===========

``DROP FUNCTION`` drops a :ref:`user-defined function
<user-defined-functions>`. The ``function_name`` and respective ``arg_type``
variables must be specified.


Parameters
==========

:IF EXISTS:
  Do not produce an error if the function doesn't exist.

:function_name:
  The name of the function to drop.

:argument_name:
  The name given to an argument.

  Function arguments do not retain names, but you can name them in your query
  for documentation purposes. Note that ``DROP FUNCTION`` will ignore argument
  names, since only the argument data types are needed to identify the
  function.

:argument_type:
  The :ref:`data type <data-types>` of an argument, if any.
