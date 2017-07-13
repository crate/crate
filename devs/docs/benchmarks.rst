Benchmarks
==========

Micro benchmarks are written using `JMH`_. They can be executed using ``gradle``::

    $ ./gradlew :core:jmh
    $ ./gradlew :sql:jmh

By default this will look for benchmarks inside ``<module>/src/jmh/java`` and
execute them.

If you want to execute specific benchmarks you can use the jar::

    $ ./gradlew :sql:jmhJar
    $ java -jar sql/build/libs/crate-sql-jmh.jar <benchmarkMethodName>

Results will be generated into ``$buildDir/reports/jmh``.

If you're writing new benchmarks take a look at this `JMH introduction`_ and
those `JMH samples`_.


End-to-end Benchmarks
=====================

Version independent benchmarks which can be written using regular SQL
statements are in the `crate-benchmarks`_ repository.


.. _JMH introduction: http://java-performance.info/jmh/
.. _JMH samples: http://hg.openjdk.java.net/code-tools/jmh/file/tip/jmh-samples/src/main/java/org/openjdk/jmh/samples/
.. _JMH: http://openjdk.java.net/projects/code-tools/jmh/
.. _crate-benchmarks: https://github.com/crate/crate-benchmarks
