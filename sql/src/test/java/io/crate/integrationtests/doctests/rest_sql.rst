====================
REST SQL action test
====================

Make sure that all exposed exceptions are a ``SQLActionException``.

Issue a request without a body::

    >>> res = post('/_sql?error_trace')
    >>> print res
    HTTP Error 400: Bad Request

    >>> print_json(res.content)
    {
        "error": {
            "code": 4000,
            "message": "SQLActionException[missing request body]"
        },
        "error_trace": null
    }

Issue a request with invalid SQL::

    >>> data = {'stmt': 'select foo from table bla'}
    >>> res = post('/_sql?error_trace', data=data)
    >>> print res
    HTTP Error 400: Bad Request

    >>> print_json(res.content)
    {
        "error": {
            "code": 4000,
            "message": "SQLActionException[line 1:17: no viable alternative at input 'table']"
        },
        "error_trace": "io.crate.exceptions.SQLParseException: line 1:17: no viable alternative at input 'table'\n\tat ..."
    }
