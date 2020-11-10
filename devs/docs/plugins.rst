=======
Plugins
=======

CrateDB implements a plugin loading infrastructure. To add functionalities,
you can develop plugins for CrateDB.

A plugin must:

- implement the ``io.crate.Plugin`` interface
- register its implementation at ``META-INF/services/io.crate.Plugin`` so that
  CrateDB plugin load can find it

Refer to our `CrateDB example plugin`_ for more details.


Plugin CrateDB Dependency
=========================

In order to develop a plugin against a CrateDB release, a dependency to the
CrateDB's server libraries must be set up.


Gradle
------

Define bintray (jCenter) repository:

.. code-block:: groovy

   repositories {
       jcenter()
   }

Add CrateDB to compile dependencies:

.. code-block:: groovy

  dependencies {
      compile 'io.crate:crate:<VERSION>'
  }


Maven
-----

Add bintray (jCenter) as a repository to your maven ``settings.xml``:

.. code-block:: xml

  <profiles>
    <profile>
      <repositories>
        <repository>
          <snapshots>
            <enabled>false</enabled>
          </snapshots>
          <id>central</id>
          <name>bintray</name>
          <url>http://jcenter.bintray.com</url>
        </repository>
      </repositories>
      <id>bintray</id>
    </profile>
  </profiles>

  <activeProfiles>
    <activeProfile>bintray</activeProfile>
  </activeProfiles>

Add CrateDB as a dependency:

.. code-block:: xml

  <dependencies>
    <dependency>
      <groupId>io.crate</groupId>
      <artifactId>crate</artifactId>
      <version>0.49.0</version>
    </dependency>
  </dependencies>


Plugin Loading
==============

Loading of plugins is done by CrateDB by searching all class path element
resources for a ``META-INF/services/io.crate.Plugin`` file.

Inside this file, one line is allowed to define the full qualified class
name which implements the `Plugin interface`_. This is similar to Java's
ServiceLoader.


Constructor with ``Settings`` argument
--------------------------------------

CrateDB passes a ``Settings`` instance to the plugin implementation constructor
if such a constructor exists. Otherwise, an empty constructor is used. By using
the ``Settings`` instance, a plugin can process existing settings and/or
implement its own custom setting entries.

The `CrateDB example plugin`_ makes use of that to implement a custom setting.

.. highlight:: java


Plugin Interface
================

CrateDB uses the `Guice`_ module binding concept and so do plugins. As
described in the ``io.crate.Plugin`` interface, a plugin can load several
module types by implementing relevant methods:

 - lifecycle services
 - node level modules

This enables plugin developers to access a lot of functionality. But that comes
at the price of API stability. Most of the components in CrateDB are considered
internal and may change with any version, including hotfix versions.

The main purpose for the plugins right now is to add additional scalar
functions or aggregation functions. An example of a plugin that does that is
`CrateDB example plugin`_.


Installing a Plugin
===================

Installing a plugin is done by copying the plugin's JAR file(s) into the class
path or to one of the following places:

 - <CRATE_HOME>/plugins/
 - <CRATE_HOME>/plugins/<SOME_PLUGIN_NAME>/
 - <CRATE_HOME>/plugins/<SOME_PLUGIN_NAME>/lib/


.. _CrateDB example plugin: https://github.com/crate/crate-example-plugin
.. _Guice: https://github.com/google/guice
