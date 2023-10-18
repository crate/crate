======================
Git & Release workflow
======================

Git worktree
============

See how you can use `git worktree`_ to work conveniently with release branches.


Git workflow
============

- Development happens in feature branches. Everything that goes into the
  master branch must be reviewed.

- PRs should be small but self-contained. "Dead code" (features that are not
  integrated) cannot go into the master branch.

- Commits must be self-contained. Each commit must compile and tests should
  pass (beware of flaky tests). This is required to be able to use ``git bisect``.


Release workflow
================

A "development cycle" ends with a feature freeze and an initial release
candidate (RC). This RC is based on master, but looks like a regular (stable)
release. We use ``X.Y.Z`` version numbers, without ``-RC`` suffix or similar RC
indication. These release candidates are available in our testing repositories.
They are not suitable for production and there is no rolling upgrade support.

For each release, we:

- Create a "prepare release" commit. This commit updates the version-info,
  finalizes the release notes and is tagged.
- Create a "version bump" commit immediately afterwards. This re-enables the
  ``SNAPSHOT`` flag and increases the version number.

For example::

    - Prepare release 2.1.0
    - Version bump (2.1.1-SNAPSHOT)
    - [... fixes ...]
    - Prepare release 2.1.1
    - Version bump (2.1.2-SNAPSHOT)
    - [... repeat ...]

During this feature freeze, we can no longer merge new features into master.
We focus on fixes, performance improvements, or other cleanups. At some point,
we will lift the feature freeze.

At this point, a ``major.minor`` release branch is created and we bump the
``minor`` version on master.

When the number of bugs discovered in the release candidates decrease enough,
it will be declared as stable and moved into the stable repositories, marking
the first stable release of this ``minor`` version.

This can happen either in master during the feature freeze, or after the switch
to a release branch.


Tagging a release
=================

Before creating a new distribution, a new version and tag should be created:

- Update ``CURRENT`` in ``org.elasticsearch.Version``

- Prepare the release notes from the ``CHANGES.txt`` file

- Commit your changes with a message like "prepare release x.y.z"

- Push to origin

- Create a tag by running ``./devs/tools/create_tag.sh``

- Create a "version bump" commit. (See workflow description above)

To build a release tarball, run::

    $ ./mvnw clean package -DskipTests=true -P release

This task runs the ``distTar`` task but also checks that the output of
``git describe --tag`` matches the current version of CrateDB.

The resulting tarball and zip file will be written to the
``./app/build/distributions`` directory.

We also have a Jenkins_ job that will build the tarball.


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
checkbox, and then saving the changes.


.. _Jenkins: http://jenkins-ci.org/
.. _versions hosted on ReadTheDocs: https://readthedocs.org/projects/crate/versions/
.. _git worktree: basics.rst#work-with-release-branches
