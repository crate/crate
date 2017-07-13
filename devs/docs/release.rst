Preparing a New Release
=======================

Before creating a new distribution, a new version and tag should be created:

- Update ``CURRENT`` in ``io.crate.Version``

- Prepare the release notes from the ``CHANGES.txt`` file

- Commit your changes with a message like "prepare release x.y.z"

- Push to origin

- Create a tag by running ``./devtools/create_tag.sh``

You can build a release tarball like so::

    $ ./gradlew release

This task runs the ``distTar`` task but also checks that the output of ``git
describe --tag`` matches the current version of CrateDB.

The resulting tarball and zip file will be written to the
``./app/build/distributions`` directory.

We have a Jenkins_ job that will build the tarball for you.


.. _Jenkins: http://jenkins-ci.org/
