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

Issue a request with invalid data payload::

    >>> data = {'foo': 'bar'}
    >>> res = post('/_sql?error_trace', data=data)
    >>> print res
    HTTP Error 400: Bad Request

    >>> print_json(res.content)
    {
        "error": {
            "code": 4000,
            "message": "SQLActionException[Failed to parse source [{\"foo\": \"bar\"}]]"
        },
        "error_trace": "io.crate.exceptions.SQLParseException: Failed to parse source [{\"foo\": \"bar\"}]\n\tat ..."
    }
