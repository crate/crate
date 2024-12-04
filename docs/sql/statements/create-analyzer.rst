.. highlight:: psql
.. _ref-create-analyzer:

===================
``CREATE ANALYZER``
===================

Define a new fulltext analyzer.

.. rubric:: Table of contents

.. contents::
   :local:

Synopsis
========

::

    CREATE ANALYZER analyzer_name EXTENDS parent_analyzer_name
        WITH ( override_parameter [= value] [, ... ] )

::

    CREATE ANALYZER analyzer_name (
        [ TOKENIZER
          {
              tokenizer_name
            | custom_name WITH ( type = tokenizer_name, tokenizer_parameter [= value] [, ... ] )
          }
        ]
        [ TOKEN_FILTERS (
            {
                token_filter_name
              | custom_name WITH ( type = token_filter_name, token_filter_parameter [= value] [, ... ] )
            }
            [, ... ]
          )
        ]
        [ CHAR_FILTERS (
            {
                char_filter_name
              | custom_name WITH ( type = char_filter_name, char_filter_parameter [= value] [, ... ] )
            }
            [, ... ]
          )
        ]
    )

Description
===========

``CREATE ANALYZER`` specifies a whole analyzer chain for use in fulltext
searches. It is possible to extend an existing analyzer or define a new
analyzer chain from scratch. For examples and detailed explanation see
:ref:`create_custom_analyzer`.

.. CAUTION::

    If ``analyzer_name`` already exists, its definition is updated, but 
    existing tables will continue to use the old definition.

Parameters
==========

:analyzer_name:
  The globally unique name of the analyzer being created.

:parent_analyzer_name:
  The name of the analyzer to inherit defaults from.

:override_parameter:
  The name of a parameter of the parent analyzer which should be
  assigned a new value to.

:tokenizer_name:
  The name of a builtin tokenizer to be used.

:tokenizer_parameter:
  A name of a parameter for a given tokenizer.

:token_filter_name:
  The name of a builtin token filter to be used.

:token_filter_parameter:
  A name of a parameter for a given token filter.

:char_filter_name:
  The name of a builtin char filter to be used.

:char_filter_parameter:
  A name of a parameter for a given char filter.

:custom_name:
  A custom unique name needed when defining custom
  tokenizers/token_filter/char_filter.

