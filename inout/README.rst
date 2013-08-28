==========================
Elasticsearch InOut Plugin
==========================

This Elasticsearch plugin provides the ability to export data by query
on server side, by outputting the data directly on the according node.
The export can happen on all indexes, on a specific index or on a specific
document type.

The data will get exported as one json object per line::

    {"_id":"id1","_source":{"type":"myObject","value":"value1"},"_version":1,"_index":"myIndex","_type":"myType"}
    {"_id":"id2","_source":{"type":"myObject","value":"value2"},"_version":2,"_index":"myIndex","_type":"myType"}

The exported data can also be imported into elasticsearch again. The import is
happening on each elasticsearch node by processing the files located in the
specified directory.


Examples
========

Below are some examples demonstrating what can be done with the elasticsearch
inout plugin. The example commands require installation on a UNIX system.
The plugin may also works with different commands on other operating
systems supporting elasticsearch, but is not tested yet.

Export data to files in the node's file system. The filenames will be expanded
by index and shard names (p.e. /tmp/dump-myIndex-0)::

    curl -X POST 'http://localhost:9200/_export' -d '{
        "fields": ["_id", "_source", "_version", "_index", "_type"],
        "output_file": "/tmp/es-data/dump-${index}-${shard}"
    }
    '

Do GZIP compression on file exports::

    curl -X POST 'http://localhost:9200/_export' -d '{
        "fields": ["_id", "_source", "_version", "_index", "_type"],
        "output_file": "/tmp/es-data/dump-${index}-${shard}.gz",
        "compression": "gzip"
    }
    '

Pipe the export data through a single argumentless command on the corresponding
node, like `cat`. This command actually returns the export data in the JSON
result's stdout field::

    curl -X POST 'http://localhost:9200/_export' -d '{
        "fields": ["_id", "_source", "_version", "_index", "_type"],
        "output_cmd": "cat"
    }
    '

Pipe the export data through argumented commands (p.e. a shell script, or
provide your own sophisticated script on the node). This command will
result in transforming the data to lower case and write the file to the
node's file system::

    curl -X POST 'http://localhost:9200/_export' -d '{
        "fields": ["_id", "_source", "_version", "_index", "_type"],
        "output_cmd": ["/bin/sh", "-c", "tr [A-Z] [a-z] > /tmp/outputcommand.txt"]
    }
    '

Limit the exported data with a query. The same query syntax as for search can
be used::

    curl -X POST 'http://localhost:9200/_export' -d '{
        "fields": ["_id", "_source", "_version", "_index", "_type"],
        "output_file": "/tmp/es-data/query-${index}-${shard}",
        "query": {
            "match": {
                "someField": "someValue"
            }
        }
    }
    '

Export only objects of a specifix index::

    curl -X POST 'http://localhost:9200/myIndex/_export' -d '{
        "fields": ["_id", "_source", "_version", "_type"],
        "output_file": "/tmp/es-data/dump-${index}-${shard}"
    }
    '

Export only objects of a specific type of an index::

    curl -X POST 'http://localhost:9200/myIndex/myType/_export' -d '{
        "fields": ["_id", "_source", "_version"],
        "output_file": "/tmp/es-data/dump-${index}-${shard}"
    }
    '

Import data from previously exported data into elastic search. This can be for
example a new set up elasticsearch server with empty indexes. Take care to
have the indexes prepared with correct mappings. The files must reside on the
file system of the elastic search node(s)::

    curl -X POST 'http://localhost:9200/_import' -d '{
        "directory": "/tmp/es-data"
    }
    '

Import data of gzipped files::

    curl -X POST 'http://localhost:9200/_import' -d '{
        "directory": "/tmp/es-data",
        "compression": "gzip"
    }
    '

Import data into a specific index. Can be used if no _index is given in the
export data or to force data of other indexes to be imported into a specific
index::

    curl -X POST 'http://localhost:9200/myNewIndex/_import' -d '{
        "directory": "/tmp/es-data"
    }
    '

Import data into a specific type of an index::

    curl -X POST 'http://localhost:9200/myNewIndex/myType/_import' -d '{
        "directory": "/tmp/es-data"
    }
    '

Use a regular expression to filter imported file names (e.g. for specific
indexes)::

    curl -X POST 'http://localhost:9200/_import' -d '{
        "directory": "/tmp/es-data",
        "file_pattern": "dump-myindex-(\\d).json"
    }
    '


Exports
=======

Elements of the request body
----------------------------

``fields``
~~~~~~~~~~

A list of fields to export. Describes which data is exported for every
object. A field name can be any property that is defined in the index/type
mapping with ``"store": "yes"`` or one of the following special fields
(prefixed with _):

* ``_id``: Delivers the ID of the object
* ``_index``: Delivers the index of the object
* ``_routing``: Delivers the routing value of the object
* ``_source``: Delivers the stored JSON values of the object
* ``_timestamp``: Delivers the time stamp when the object was created (or the
  externally provided timestamp). Works only if the _timestamp field is enabled
  and set to ``"store": "yes"`` in the index/type mapping of the object.
* ``_ttl``: Delivers the expiration time stamp of the object if the _ttl field
  is enabled in the index/type mapping.
* ``_type``: Delivers the document type of the object
* ``_version``: Delivers the current version of the object

Example assuming that the properties ``name`` and ``address`` are defined
in the index/type mapping with the property ``"store": "yes"``::

    "fields": ["_id", "name", "address"]

The ``fields`` element is required in the POST data of the request.

``output_cmd``
~~~~~~~~~~~~~~

    "output_cmd": "cat"

    "output_cmd": ["/location/yourcommand", "argument1", "argument2"]

The command to execute. Might be defined as string or as array. The
content to export will get piped to Stdin of the command to execute.
Some variable substitution is possible (see Variable Substitution)

- Required (if ``output_file`` has been omitted)

``output_file``
~~~~~~~~~~~~~~~

    "output_file": "/tmp/dump"

A path to the resulting output file. The containing directory of the
given ``output_file`` has to exist. The given ``output_file`` MUST NOT exist,
unless the parameter ``force_overwrite`` is set to true.

If the path of the output file is relative, the files will be stored relative
to each node's first `node data location`, which is usually a subdirectory of
the configured data location. This absolute path can be seen in the JSON
response of the request. If you don't know where this location is, you can do
a dry-run with the ``explain`` element set to ``true`` to find out.

Some variable substitution in the output_file's name is also possible (see
Variable Substitution).

- Required (if ``output_cmd`` has been omitted)

``force_overwrite``
~~~~~~~~~~~~~~~~~~~

    "force_overwrite": true

Boolean flag to force overwriting existing ``output_file``. This option only
make sense if ``output_file`` has been defined.

- Optional (defaults to false)

``explain``
~~~~~~~~~~~

    "explain": true

Option to evaluate the command to execute (like dry-run).

- Optional (defaults to false)

``compression``
~~~~~~~~~~~~~~~

    "compression": "gzip"

Option to activate compression to the output. Works both whether
``output_file`` or ``output_cmd`` has been defined. Currently only the
``gzip`` compression type is available. Omitting the option will result
in uncompressed output to files or processes.

- Optional (default is no compression)

``query``
~~~~~~~~~

The query element within the export request body allows to define a
query using the Query DSL. See
http://www.elasticsearch.org/guide/reference/query-dsl/

- Optional

``settings``
~~~~~~~~~~~~

    "settings": true

Option to generate an index settings file next to the data files on all
corresponding shards. The generated settings file has the generated name of
the output file with the ``.settings`` extension. This option is only possible
if the option ``output_file`` has been defined.

- Optional (defaults to false)

``mappings``
~~~~~~~~~~~~

    "mappings": true

Option to generate an index mapping file next to the data files on all
corresponding shards. The generated mapping file has the generated name of
the output file with an ``.mapping`` extension. This option is only possible
if the option ``output_file`` has been defined.

- Optional (defaults to false)


Get parameters
--------------

The api provides the general behavior of the rest API. See
http://www.elasticsearch.org/guide/reference/api/

Preference
~~~~~~~~~~

Controls a preference of which shard replicas to execute the export
request on. Different than in the search API, preference is set to
"_primary" by default. See
http://www.elasticsearch.org/guide/reference/api/search/preference/


Variable Substitution
---------------------

The following placeholders will be replaced with the actual value in
the ``output_file`` or ``output_cmd`` fields:

* ``${cluster}``: The name of the cluster
* ``${index}``: The name of the index
* ``${shard}``: The id of the shard


JSON Response
-------------

The _export query returns a JSON response with information about the export
status. The output differs a bit whether an output command or an output file
is given in the request body.

Output file JSON response
~~~~~~~~~~~~~~~~~~~~~~~~~

The JSON response may look like this if an output file is given in the
request body::

    {
        "exports" : [
            {
                "index" : "myIndex",
                "shard" : 0,
                "node_id" : "the_node_id",
                "numExported" : 5,
                "output_file" : "/tmp/dump-myIndex-0"
            }
        ],
        "totalExported" : 5,
        "_shards" : {
            "total" : 2,
            "successful" : 1,
            "failed" : 1,
            "failures" : [
                {
                    "index" : "myIndex",
                    "shard" : 1,
                    "reason" : "..."
                }
            ]
        }
    }

Output command JSON response
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The JSON response may look like this if an output command is given in the
request body::

    {
        "exports" : [
            {
                "index" : "myIndex",
                "shard" : 0,
                "node_id" : "the_node_id",
                "numExported" : 5,
                "output_cmd" : [
                    "/bin/sh",
                    "-c",
                    "tr [A-Z] [a-z] > /tmp/outputcommand.txt"
                ],
                "stderr" : "",
                "stdout" : "",
                "exitcode" : 0
            }
        ],
        "totalExported" : 5,
        "_shards" : {
            "total" : 2,
            "successful" : 1,
            "failed" : 1,
            "failures": [
                {
                    "index" : "myIndex",
                    "shard" : 1,
                    "reason" : "..."
                }
            ]
        }
    }

.. hint::

    - ``exports``: List of successful exports
    - ``totalExported``: Number of total exported objects
    - ``_shards``: Shard information
    - ``index``: The name of the exported index
    - ``shard``: The number of the exported shard
    - ``node_id``: The node id where the export happened
    - ``numExported``: The number of exported objects in the shard
    - ``output_file``: The file name of the output file with substituted variables
    - ``failures``: List of failing shard operations
    - ``reason``: The error report of a specific shard failure
    - ``output_cmd``: The executed command on the node with substituted variables
    - ``stderr``: The first 8K of the standard error log of the executed command
    - ``stdout``: The first 8K of the standard output log of the executed command
    - ``exitcode``: The exit code of the executed command


Imports
=======

Import data
-----------

The import data requires the same format as the format that is generated by
the export. So every line in the import file represents an object in JSON
format.

The ``_source`` field is required for a successful import of an object. If
the ``_id`` field is not given, a random id is generated for the object.
Also the ``_index`` and ``_type`` fields are required, as long as they are not
given in the request URI (p.e. http://localhost:9200/<index>/<type>/_index).

Further optional fields are ``_routing``, ``_timestamp``, ``_ttl`` and
``_version``. See the ``fields`` section on export for more details on the
fields.


Elements of the request body
----------------------------

``directory``
~~~~~~~~~~~~~

Specifies the directory where the files to be imported reside. Every single
node of the cluster imports files from that directory on it's file system.

If the directory is a relative path, it is based on the absolute path of each
node's first `node data location`. See ``output_file`` in export documentation
for more information.

``compression``
~~~~~~~~~~~~~~~

    "compression": "gzip"

Option to activate decompression on the import files. Currently only the
``gzip`` compression type is available.

- Optional (default is no decompression)

``file_pattern``
~~~~~~~~~~~~~~~~

    "file_pattern": "index-(.*)-(\\d).json"

Option to import only files with a given regular expression. Take care of
double escaping, as the JSON is decoded too in the process. For more
information on regular expressions visit http://www.regular-expressions.info/

- Optional (default is no filtering)

``settings``
~~~~~~~~~~~~

    "settings": true

Option to import index settings. All files in the import directory with an
eponymic data file without the ``.settings`` extension will be handled. Also
use the ``file_pattern`` option to reduce imported settings files. The format
of a settings file is the same as the JSON output of ``_settings`` GET requests.

- Optional (defaults to false)

``mappings``
~~~~~~~~~~~~

    "mappings": true

Option to import index mappings. All files in the import directory with an
eponymic data file without the ``.mapping`` extension will be handled. Also
use the ``file_pattern`` option to reduce imported mapping files. The format
of a mapping file is the same as the JSON output of ``_mapping`` GET requests.

- Optional (defaults to false)


JSON Response
-------------

The JSON response of an import may look like this::

    {
        "imports" : [
            {
                "node_id" : "7RKUKxNDQlq0OzeOuZ02pg",
                "took" : 61,
                "imported_files" : [
                    {
                        "file_name" : "dump-myIndex-1.json",
                        "successes" : 150,
                        "failures" : 0
                    },
                    {
                        "file_name" : "dump-myIndex-2.json",
                        "successes" : 149,
                        "failures" : 1,
                        "invalidated" : 1
                    }
                ]
            },
            {
                "node_id" : "IrMCOlKCTtW4aDhjXiYzTw",
                "took" : 63,
                "imported_files" : [
                    {
                        "file_name" : "dump-myIndex-3.json",
                        "successes" : 150,
                        "failures" : 0
                    }
                ]
            }
        ],
        "failures" : [
            {
                "node_id" : "OATwHz48TEOshAISZlepcA",
                "reason" : "..."
            }
        ]
    }

.. hint::

    - ``imports``: List of successful imports
    - ``node_id'': The node id where the import happened
    - ``took``: Operation time of all imports on the node in milliseconds
    - ``imported_files``: List of imported files in the import directory of the node's file system
    - ``file_name``: File name of the handled file
    - ``successes``: Number of successfully imported objects per file
    - ``failures`` (in imported_files list): Number of not imported objects because of a failure
    - ``invalidated``: Number of not imported objects because of invalidation (time to live exceeded)
    - ``failures`` (in root): List of failing node operations
    - ``reason``: The error report of a specific node failure


Dump
====

The idea behind dump is to export all relevant data to recreate the
cluster as it was at the time of the dump.

The basic usage of the endpoint is:

    curl -X POST 'http://localhost:9200/_dump'

All data (including also settings and mappings) will get saved to a subfolder
within each nodes data directory.

It's possible to call _dump on root level, on index level or on type
level.

Elements of the request body
----------------------------

``directory``
~~~~~~~~~~~~~

The directory option defines where to store exported files.  If the
directory is a relative path, it is based on the absolute path of each
node's first `node data location`. See ``output_file`` in export
documentation for more information. If the directory was omitted the
default location `dump` within the node data location will be used.

``force_overwrite``
~~~~~~~~~~~~~~~~~~~

    "force_overwrite": true

Boolean flag to force overwriting existing ``output_file``. This
option is identical to the force_overwrite option of the _export
endpoint.


Restore
=======

Dumped data is intended to get restored. This can be done by the _restore
endpoint:

    curl -X POST 'http://localhost:9200/_restore'

It's possible to call _restore on root level, on index level or on type
level.

Elements of the request body
----------------------------

``directory``
~~~~~~~~~~~~~

Specifies the directory where the files to be restored reside. See
``directory`` in import documentation for more details. If the
directory was omitted the default location `dump` within the node data
location will be used.

``settings`` and ``mappings``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Defaults to true on restore. See the Import documentation for more details.


Reindex
=======

The ``_reindex`` endpoint can reindex documents of a given search query.

Reindex all indexes::

    curl -X POST 'http://localhost:9200/_reindex'

Reindex a specific index::

    curl -X POST 'http://localhost:9200/myIndex/_reindex'

Reindex documents of a specified query::

    curl -X POST 'http://localhost:9200/myIndex/aType/_reindex' -d '{
        "query": {"text": {"name": "tobereindexed"}}
    }'

An example can be found in the `Reindex DocTest <src/test/python/reindex.rst>`_.


Search Into
===========

Via the ``_search_into`` endpoint it is possible to put the result of
a given query directly into an index::

    curl -X POST 'http://localhost:9200/oldindex/_search_into -d '{
        "fields": ["_id", "_source", ["_index", "'newindex'"]]
    }'

An example can be found in the `Search Into DocTest
<src/test/python/search_into.rst>`_.


Installation
============

* Clone this repo with git clone
  git@github.com:crate/elasticsearch-inout-plugin.git
* Checkout the tag (find out via git tag) you want to build with
  (possibly master is not for your elasticsearch version)
* Run: mvn clean package -DskipTests=true – this does not run any unit
  tests, as they take some time. If you want to run them, better run
  mvn clean package
* Install the plugin: /path/to/elasticsearch/bin/plugin -install
  elasticsearch-inout-plugin -url
  file:///$PWD/target/releases/elasticsearch-inout-plugin-$version.jar
