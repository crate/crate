================
Maven cheatsheet
================

To check new versions available for maven plugins::

    $ ./mvnw versions:display-plugin-updates |grep -- "->" |sort |uniq


To check new versions available for library dependencies::

    $ ./mvnw versions:display-dependency-updates |grep -- "->" |sort |uniq

