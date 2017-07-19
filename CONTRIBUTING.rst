============
Contributing
============

Thank you for your interest in contributing.

This document is a guideline. Don't worry too much about getting everything
perfect. We are more than happy to work with you on your contribution.

Reporting issues it the easiest way to contribute. But we also accept pull
requests for the code and for the documentation. For more information on how to
work on the code or documentation, see our `developer guide`_.

If you have any questions, please feel free to ask us on Slack_.

Reporting Issues
================

Before you report an issue, please `search the existing issues`_ to make sure
someone hasn't already reported it.

When reporting a new issue, include as much detail as possible. For some
Crate.io_ repositories, issue templates have been configured, and you can just
follow those.

For repositories without configured issue templates:

- Say what you did, what happened, and what you expected to happen

- Provide steps to reproduce the issue

- Say which OS you're using

- Say which version of CrateDB you are running

- Include logs or stacktraces

  - For example, the ``crash`` CLI client can be started with the ``-v`` option
    to get a stacktrace from the server if a SQL statement resulted in an
    unexpected error.

Pull Requests
=============

Before we can accept any pull requests we need you to agree to our CLA_.

Once that is done, we suggest you:

- Create an issue on Github to let us know that you're working on something.

- Use a feature branch, not ``master``.

- Rebase your feature branch onto ``origin/master`` before creating the pull
  request.

- Be descriptive in your PR and commit messages. What is it for? Why is it
  needed? And so on.

- Run ``./gradlew test itest`` to check that all tests pass.

- Squash related commits.

General Tips
============

Updating Your Branch
--------------------

If new commits have been added to ``master`` since you created your feature
branch, please do not merge in to your branch. Instead, rebase your branch::

    $ git fetch origin
    $ git rebase origin/master

This will apply all commits on your feature branch on top of the ``master``
branch. Any conflicts can be resolved just the same as if ``git merge`` was
used. After the conflict has been resolved, use ``git rebase --continue`` to
continue the rebase process.

Squashing Minor Commits
-----------------------

Minor commits that only fix typos or rename variables that are related to a
bigger change should be squashed into that commit.

This can be done like so::

    $ git rebase -i origin/master

This will open up a text editor where you can annotate your commits.

Generally, you'll want to leave the first commit on listed as ``pick``, or
change it to ``reword`` (or ``r`` for short) if you want to change the commit
message. And then, if you wanted to squash every subsequent commit, you could
mark them all as ``fixup`` (or ``f`` for short).

Once you're done, you can check it worked by running::

    $ git log

If you're happy, force push::

    $ git push -f

.. _CLA: https://crate.io/community/contribute/agreements/
.. _Crate.io: http://crate.io/
.. _developer guide: devs/docs/index.rst
.. _Slack: https://crate.io/docs/support/slackin/
.. _search the existing issues: https://github.com/issues?utf8=%E2%9C%93&q=is%3Aopen+is%3Aissue+user%3Acrate+
