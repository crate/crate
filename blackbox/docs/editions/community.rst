.. _community-edition:

=================
Community Edition
=================

The community edition (``CE``) of CrateDB is a distribution that excludes
enterprise modules which are licensed under a more restrictive license. The
``CE`` can be used under the terms of the Apache License version 2 without
further restrictions.

The :ref:`enterprise_features` are not available within the ``CE``.

The community edition distribution must be built from source::

   $ git clone https://github.com/crate/crate
   $ cd crate
   $ git submodule update --init
   $ [git checkout <tag> ]
   $ ./gradlew clean communityEditionDistTar

The built tarball will be available under ``app/build/distributions``.
