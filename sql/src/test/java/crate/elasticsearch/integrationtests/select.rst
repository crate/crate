==========
SQL Select
==========

    >>> import json
    >>> def sql(stmt):
    ...     payload = json.dumps
    ...     resp = post('/_sql', json.dumps(dict(stmt=stmt)))
    ...     print(json.dumps(json.loads(resp.content), indent=4, sort_keys=True))

If the '_source' field is selected, the response contains just the
'_source' field::

    >>> sql('select "_source" from locations order by "_id" limit 1')
    {
        "rows": [
            {
                "_source": {
                    "date": "1979-10-12",
                    "description": "...",
                    "kind": "Galaxy",
                    "name": "North West Ripple",
                    "position": 1
                }
            }
        ]
    }


If multiple fields are selected, the response contains all unique
fields. The `_type` field is returned by `select *` and by `select
_type`, but only listed once in the response::

    >>> sql('select *, name, "_type" from locations order by "_id" limit 1')
    {
        "rows": [
            {
                "_id": "1",
                "_index": "locations",
                "_source": {
                    "date": "1979-10-12",
                    "description": "...",
                    "kind": "Galaxy",
                    "name": "North West Ripple",
                    "position": 1
                },
                "_type": "location",
                "name": "North West Ripple"
            }
        ]
    }


If multiple fields are selected, the response contains all unique
fields.  The `_source` field is returned by `select \*` and by `select
_source`, it is listed twice in the response due to aliasing::

    >>> sql('select *, "_source" as s from locations order by "_id" limit 1')
    {
        "rows": [
            {
                "_id": "1",
                "_index": "locations",
                "_source": {
                    "date": "1979-10-12",
                    "description": "...",
                    "kind": "Galaxy",
                    "name": "North West Ripple",
                    "position": 1
                },
                "_type": "location",
                "s": {
                    "date": "1979-10-12",
                    "description": "...",
                    "kind": "Galaxy",
                    "name": "North West Ripple",
                    "position": 1
                }
            }
        ]
    }


If the `_id` field is selected, the response contains just the `_id` fields::

    >>> sql('select "_id" from locations order by "_id" limit 1')
    {
        "rows": [
            {
                "_id": "1"
            }
        ]
    }

Using `as X` on the `_id` field also works::

    >>> sql('select "_id" as id from locations order by "_id" limit 1')
    {
        "rows": [
            {
                "id": "1"
            }
        ]
    }

Using `select _id as id, _id` should return the _id field twice::

    >>> sql('select "_id" as id, "_id" from locations order by "_id" limit 1')
    {
        "rows": [
            {
                "_id": "1",
                "id": "1"
            }
        ]
    }

If the `_index` field is selected, the response contains just the `_index` fields::

    >>> sql('select "_index" from locations order by "_id"')
    {
        "rows": [
            {
                "_index": "locations"
            },
            {
                "_index": "locations"
            },
                ...
        ]
    }


If the `_type` field is selected, the response contains just the `_type` fields::

    >>> sql('select "_type" from locations order by "_id"')
    {
        "rows": [
            {
                "_type": "location"
            },
            {
                "_type": "location"
            },
                ...
        ]
    }

If the field doesn't exist null is returned::

    >>> sql('select "_ver" from locations order by "_id" limit 1')
    {
        "rows": [
            {
                "_ver": null
            }
        ]
    }

    >>> sql('select n, name from locations order by "_id" limit 1')
    {
        "rows": [
            {
                "n": null,
                "name": "North West Ripple"
            }
        ]
    }

Selecting the `version` field is also supported::

    >>> sql('select "_version" from locations order by "_id" limit 1')
    {
        "rows": [
            {
                "_version": 1
            }
        ]
    }

    >>> sql('select "_version" as v from locations order by "_id" limit 1')
    {
        "rows": [
            {
                "v": 1
            }
        ]
    }

    >>> sql('select *, "_version" from locations order by "_id" limit 1')
    {
        "rows": [
            {
                "_id": "1",
                "_index": "locations",
                "_source": {
                    "date": "1979-10-12",
                    "description": "...",
                    "kind": "Galaxy",
                    "name": "North West Ripple",
                    "position": 1
                },
                "_type": "location",
                "_version": 1
            }
        ]
    }

    >>> sql('select *, "_version", "_version" as v from locations order by "_id" limit 1')
    {
        "rows": [
            {
                "_id": "1",
                "_index": "locations",
                "_source": {
                    "date": "1979-10-12",
                    "description": "...",
                    "kind": "Galaxy",
                    "name": "North West Ripple",
                    "position": 1
                },
                "_type": "location",
                "_version": 1,
                "v": 1
            }
        ]
    }

Test that `=` returns all rows where the value is an empty string::

    >>> sql('''select name, "_id" from locations where name = '' order by "_id" limit 20''')
    {
        "rows": [
            {
                "_id": "12",
                "name": ""
            }
        ]
    }

Test that `is null` returns all rows where the value is `null`::

    >>> sql('select name, "_id" from locations where name is null order by "_id" limit 20')
    {
        "rows": [
            {
                "_id": "13",
                "name": null
            }
        ]
    }

Test that `is null` returns all rows where the field doesn't exist in the
document (in this case, all rows)::

    >>> sql('select name, "_id" from locations where invalid_field is null order by "_id" limit 20')
    {
        "rows": [
            {
                "_id": "1",
                "name": "North West Ripple"
            },
            {
                "_id": "10",
                "name": "Arkintoofle Minor"
            },
            {
                "_id": "11",
                "name": "Bartledan"
            },
            {
                "_id": "12",
                "name": ""
            },
            {
                "_id": "13",
                "name": null
            },
            {
                "_id": "2",
                "name": "Outer Eastern Rim"
            },
            {
                "_id": "3",
                "name": "Galactic Sector QQ7 Active J Gamma"
            },
            {
                "_id": "4",
                "name": "Aldebaran"
            },
            {
                "_id": "5",
                "name": "Algol"
            },
            {
                "_id": "6",
                "name": "Alpha Centauri"
            },
            {
                "_id": "7",
                "name": "Altair"
            },
            {
                "_id": "8",
                "name": "Allosimanius Syneca"
            },
            {
                "_id": "9",
                "name": "Argabuthon"
            }
        ]
    }

Test that `!=` returns all rows where the value is not empty::

    >>> sql('''select "_id", name from locations where name != '' order by "_id" limit 20''')
    {
        "rows": [
            {
                "_id": "1",
                "name": "North West Ripple"
            },
            {
                "_id": "10",
                "name": "Arkintoofle Minor"
            },
            {
                "_id": "11",
                "name": "Bartledan"
            },
            {
                "_id": "13",
                "name": null
            },
            {
                "_id": "2",
                "name": "Outer Eastern Rim"
            },
            {
                "_id": "3",
                "name": "Galactic Sector QQ7 Active J Gamma"
            },
            {
                "_id": "4",
                "name": "Aldebaran"
            },
            {
                "_id": "5",
                "name": "Algol"
            },
            {
                "_id": "6",
                "name": "Alpha Centauri"
            },
            {
                "_id": "7",
                "name": "Altair"
            },
            {
                "_id": "8",
                "name": "Allosimanius Syneca"
            },
            {
                "_id": "9",
                "name": "Argabuthon"
            }
        ]
    }

Test that `is not null` returns all rows where the value is not `null`::

    >>> sql('select "_id", name from locations where name is not null order by "_id" limit 20')
    {
        "rows": [
            {
                "_id": "1",
                "name": "North West Ripple"
            },
            {
                "_id": "10",
                "name": "Arkintoofle Minor"
            },
            {
                "_id": "11",
                "name": "Bartledan"
            },
            {
                "_id": "12",
                "name": ""
            },
            {
                "_id": "2",
                "name": "Outer Eastern Rim"
            },
            {
                "_id": "3",
                "name": "Galactic Sector QQ7 Active J Gamma"
            },
            {
                "_id": "4",
                "name": "Aldebaran"
            },
            {
                "_id": "5",
                "name": "Algol"
            },
            {
                "_id": "6",
                "name": "Alpha Centauri"
            },
            {
                "_id": "7",
                "name": "Altair"
            },
            {
                "_id": "8",
                "name": "Allosimanius Syneca"
            },
            {
                "_id": "9",
                "name": "Argabuthon"
            }
        ]
    }

    >>> sql('''select "_id", name from locations where name is not null and name != '' order by "_id" limit 20''')
    {
        "rows": [
            {
                "_id": "1",
                "name": "North West Ripple"
            },
            {
                "_id": "10",
                "name": "Arkintoofle Minor"
            },
            {
                "_id": "11",
                "name": "Bartledan"
            },
            {
                "_id": "2",
                "name": "Outer Eastern Rim"
            },
            {
                "_id": "3",
                "name": "Galactic Sector QQ7 Active J Gamma"
            },
            {
                "_id": "4",
                "name": "Aldebaran"
            },
            {
                "_id": "5",
                "name": "Algol"
            },
            {
                "_id": "6",
                "name": "Alpha Centauri"
            },
            {
                "_id": "7",
                "name": "Altair"
            },
            {
                "_id": "8",
                "name": "Allosimanius Syneca"
            },
            {
                "_id": "9",
                "name": "Argabuthon"
            }
        ]
    }
