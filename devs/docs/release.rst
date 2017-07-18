======================
Git & Release workflow
======================

- Development happens in feature branches. Everything that goes into master
  must be reviewed.

- PRs should be small but self-contained. "Dead code" (Features that aren't
  integrated) cannot go into master.

- Commits must be self-contained. Each commit must compile and tests should
  pass. (Beware flaky tests)
  This is required to be able to use ``git bisect``.

Release workflow
================


A "development cycle" ends with a feature freeze and an initial release
candidate. This RC is based on master, but looks like a regular release. We use
``X.Y.Z`` version numbers, without ``-RC`` suffix or similar RC indication.
These release candidates are available in our testing repositories. They're not
suitable for production and there is no rolling upgrade support.

There will likely be several release candidates being released, each time we:

- Create a "prepare release" commit. This commit updates the version-info,
  finalizes the release notes and is tagged.
- Create a "version bump" commit immediately afterwards. This re-enables the
  ``SNAPSHOT`` flag and increases the version number.

So for example::

    - Prepare release 2.1.0
    - Version bump (2.1.1-SNAPSHOT)
    - [... fixes ...]
    - Prepare release 2.1.1
    - Version bump (2.1.2-SNAPSHOT)
    - [... repeat ...]

During this feature freeze we can no longer merge new features into master. We
focus on fixes, performance improvements or other cleanups.


At some point we'll lift the feature freeze. At this point a ``major.minor``
release branch is created and we bump the ``minor`` version on master.


When the number of bugs discovered in the release candidates decrease enough,
it's eventually declared as stable and moved into the stable repositories,
marking the first stable release of this ``minor`` version.

This can happen either in master during the feature freeze, or after the switch
to a release branch.


Tagging a release
=================

Before creating a new distribution, a new version and tag should be created:

- Update ``CURRENT`` in ``io.crate.Version``

- Prepare the release notes from the ``CHANGES.txt`` file

- Commit your changes with a message like "prepare release x.y.z"

- Push to origin

- Create a tag by running ``./devs/tools/create_tag.sh``

- Create a "version bump" commit. (See workflow description above)

You can build a release tarball like so::

    $ ./gradlew release

This task runs the ``distTar`` task but also checks that the output of ``git
describe --tag`` matches the current version of CrateDB.

The resulting tarball and zip file will be written to the
``./app/build/distributions`` directory.


We have a Jenkins_ job that will build the tarball for you.

Archiving Docs Versions
=======================

Check the `versions hosted on ReadTheDocs`_.

We only host the docs for:

- `latest`
- `stable`
- The branch corresponding to the current testing release
- The branch corresponding to the current stable release
- The two branches preceding the branch corresponding to the current stable
  release
- The most recent branch for each major release

Sometimes you might find that there are multiple older releases that need to be
archived.

You can archive releases by selecting *Edit*, unselecting the *Active*
checkbox, and then saving.

.. _Jenkins: http://jenkins-ci.org/
.. _versions hosted on ReadTheDocs: https://readthedocs.org/projects/crate-dbal/versions/
