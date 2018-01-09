==========
Benchmarks
==========

Micro benchmarks are written using `JMH`_. They can be executed by using the ``jmhJar``.

Build the jar::

    $ ./gradlew :benchmarks:jmhJar

Then run the benchmarks::

    $ java -jar benchmarks/build/libs/crate-benchmarks-all.jar

This will execute all benchmarks which are in ``benchmarks/src/test/java``.

If you want to execute specific benchmarks you can provide a filter argument::

    $ java -jar benchmarks/build/libs/crate-benchmarks-all.jar <benchmarkMethodName | benchmarkClassName[.methodName]>

To save the results to a file use the ``-rf`` and ``-rff`` options::

    $ java -jar benchmarks/build/libs/crate-benchmarks-all.jar -rf json -rff benchmarks/build/jmh.json

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
