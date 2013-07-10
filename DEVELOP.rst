===========
DEVELOPMENT
===========

Submodules
==========

Init/Update
-----------

In order to build the project the first time it is necessary to
initialize and update all submodules. Which is done via the
following command from within the crate repository::

 $ git submodule update --init


Version Handling
----------------

Dependencies which has been included into the project by git submodules
requires an unconventional version handling. To update such a submodule
to a specific version `cd` into the particular directory and reset the
package to the specific tag.

 $ cd <submodule>
 $ git fetch
 $ git reset --hard <tag>

The submodule will now point to the related commit of the tag.

Build
=====

In pom.xml update the <version> tag of the package. In
elasticsearch-crate-plugin/pom.xml update the <crate.version> tag to
the same version.

Building a tarball and a zip is done by maven with the command::

    >>> mvn clean package

Resulting tarball and zip will reside in the folder ``releases``.
