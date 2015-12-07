Contributing to Crate
=====================

Thanks for considering contributing to crate.

The steps outlined in this document should be seen as guidelines. These are NOT
strict rules and we won't blame anyone who tries to contribute.  By following
these things you'll just make our lives a tad bit easier, but if you don't
follow them we'll still be happy about your contribution.

The easiest way to contribute is either by filling issues on `Github
<https://github.com/crate/crate/issues>`_ or by contributing to our
documentation.

For more information on how to do that please refer to the documentation
section in the ``DEVELOP.rst``.

If you have any questions don't hesitate to ask us in our public room on 
HipChat_.

.. _HipChat: https://www.hipchat.com/g7Pc2CYwi


Reporting an issue
------------------

- Search existing issues before raising a new one.

- include as much details as possible. This might include:

  - Which OS you're using.

  - Which version you've been running.

  - Logs/Stacktrace of the error that occurred.
    (For example ``crash`` the command-line client can be started with the `-v`
    option to get a stacktrace from the server if a SQL statement resulted in
    an error)

  - Steps to reproduce.


Pull requests
-------------

Before we can accept any pull requests to Crate we need you to agree to our
CLA_. Once that is done, we suggest to continue as follows:

- Add an issue on Github and let us know that you're working on something.

- Use a feature branch, not master.

- Rebase your feature branch onto origin/master before raising the PR

- Be descriptive in your PR and commit messages. What is it for, why is it
  needed, etc.

- Make sure the tests pass. (`./gradlew test itest`)

- Squash related commits

.. _CLA: https://crate.io/community/contribute/agreements/


Rebase
------

If while you've been working in the feature branch new commits were added to
the master branch please don't merge them but use rebase::

    git fetch origin
    git rebase origin/master

This will apply all commits on your feature branch on top of the master branch.
Any conflicts can be resolved just the same as if ``git merge`` was used. After
the conflict has been resolved use ``git rebase --continue`` to continue the
rebase process.


Squash
------

Minor commits that only fix typos or rename variables that are related to a
bigger change should be squashed into that commit.

This can be done using ``git rebase -i ( <hash> | <branch> )``

For example while working on a feature branch you'd use::

    git add .
    git commit -m "implemented feature XY"

    # push and ask for a merge/review

    git add .
    git commit --fixup $(git rev-parse HEAD)

    # push and ask for a merge/review

    git add .
    git commit --fixup $(git rev-parse HEAD)

    git rebase -i origin/master

``git commit --fixup`` will mark the commit as a fixup relating to the commit
HEAD currently points to.

This is useful because ``git rebase -i`` will then automatically recognize the
fixup commits and mark them to squash. But in order for that to work the
``autosquash`` setting has to be enabled in the ``.gitconfig``::

    git config --global rebase.autosquash true
