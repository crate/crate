==========
Benchmarks
==========

Microbenchmarks are written using `JMH`_. They can be executed by using the
``:benchmarks:run``::

    $ ./gradlew :benchmarks:run

If you want to execute specific benchmarks you can provide a filter argument::

    $ ./gradlew :benchmarks:run --args="<benchmarkMethodName | benchmarkClassName>"

To save the results to a file, use the ``-rf`` and ``-rff`` options::

    $ ./gradlew :benchmarks:run --args="-rf json -rff /tmp/jmh.json"

If you are writing new benchmarks, take a look at this `JMH introduction`_ and
these `JMH samples`_.


End-to-end Benchmarks
=====================

Version-independent benchmarks which can be written using regular SQL
statements are in the `crate-benchmarks`_ repository.


.. _crate-benchmarks: https://github.com/crate/crate-benchmarks
.. _JMH introduction: http://java-performance.info/jmh/
.. _JMH samples: http://hg.openjdk.java.net/code-tools/jmh/file/tip/jmh-samples/src/main/java/org/openjdk/jmh/samples/
.. _JMH: http://openjdk.java.net/projects/code-tools/jmh/
