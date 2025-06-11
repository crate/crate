.. highlight:: psql

.. _ref-create-function:

===================
``CREATE FUNCTION``
===================

Create a new :ref:`function <user-defined-functions>`.

Synopsis
========

::

    CREATE [OR REPLACE] FUNCTION function_name
        ( [ [argument_name] argument_type ] [, ...] ] )
    RETURNS return_type
    LANGUAGE language_name
    AS 'definition'


Description
===========

``CREATE FUNCTION`` creates a new :ref:`user-defined function
<user-defined-functions>`.

``CREATE OR REPLACE FUNCTION`` will either create a new function, or replace an
existing function.

The signature of the function is defined by its schema, name, and input
arguments.

You can overload functions by defining two functions of the same name, but with
a different set of input arguments.


Parameters
==========

:function_name:
  The name of the function to create.

:argument_name:
  The optional name given to an argument. Function arguments do not retain
  names, but you can name them in your query for documentation purposes.

:argument_type:
  The :ref:`data type <data-types>` of a given argument.

:return_type:
  The returned data type of the function. The return type can be any
  supported type.

:language_name:
  The registered language which should be used for the function.

:definition:
  A :ref:`string <string_literal>` defining the body of the function.
