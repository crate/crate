==========
Benchmarks
==========

Microbenchmarks are written using `JMH`_. To execute them, first build the JAR::

    $ ./mvnw package -DskipTests=true -P benchmark

Then run the JAR::

    $ java -jar benchmarks/target/benchmarks.jar

The `java` version needs to be equal or greater than the configured toolchain.
Maven automatically downloads the JDK when it is missing and it should be
available in ``~/.m2/jdks/jdk-<version>``.

To get the configured JDK version, use::

    $ ./mvnw help:evaluate -Dexpression=versions.jdk -q -DforceStdout

If you want to execute specific benchmarks you can provide a filter argument::

    $ java -jar benchmarks/target/benchmarks.jar <benchmarkMethodName | benchmarkClassName>"

To save the results to a file, use the ``-rf`` and ``-rff`` options::

    $ java -jar benchmarks/target/benchmarks.jar -rf json -rff /tmp/jmh.json

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
