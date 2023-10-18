============
Contributing
============

Thank you for your interest in contributing.

This document is a guideline. Don't worry about getting everything perfect.
We are happy to work with you on your contribution.

`Upvoting existing issues`_, `reporting new issues`_, or `giving feedback`_
about your experience are the easiest ways to contribute.

We also accept pull requests for changes to the code and to the documentation.
For more information on how to do this, read our `developer guide`_.

If you have any questions, please reach out via any of our `support channels`_.


Upvoting Issues
===============

An easy way to contribute is to upvote existing issues that are relevant to
you. This will give us a better idea what is important for you and other users.

Please avoid content-less ``+1`` comments but instead use the emoji reaction to
upvote with a 👍. This allows people to `sort issues by reaction`_ and doesn't
spam the maintainers.


Reporting Issues
================

Before you report an issue, please `search the existing issues`_ to make sure
someone hasn't already reported it.

When reporting a new issue, include as much detail as possible. For some
Crate.io_ repositories, issue templates have been configured.

For repositories without configured issue templates, include:

- What you did, what happened, and what you expected to happen

- Steps to reproduce the issue

- Which operating system you're using

- Which version of CrateDB you're running

- Logs or stacktraces

  - For example, the ``crash`` CLI client can be started with the ``-v`` option
    to get a stacktrace from the server if a SQL statement resulted in an
    unexpected error


Tell us about your experience
=============================

You don't have to create a detailed bug report or request a new feature to make
a valuable contribution. Giving us feedback about your experience with CrateDB is
incredibly valuable as well. Please `get in touch`_ with us to tell us what you
like and don't like about CrateDB.


Pull Requests
=============

Before we can accept any pull requests, we need you to agree to our CLA_.

Once that is complete, you should:

- Create an issue on GitHub to let us know that you're working on the issue.

- Use a feature branch and not ``master``.

- Rebase your feature branch onto ``origin/master`` before creating the pull
  request.

- Be descriptive in your PR and commit messages. What is it for? Why is it
  needed? And so on.

- If applicable, run ``./mvnw test && ./blackbox/bin/test`` to check that all
  tests pass.

- Squash related commits.


General Tips
============


Meaningful Commit Messages
--------------------------

Please choose a meaningful commit message. The commit message is not only
valuable during the review process, but can be helpful for reasoning about
any changes in the code base. For example, IntelliJ's "Annotate" feature,
brings up the commits which introduced the code in a source file. Without
meaningful commit messages, the commit history does not provide any valuable
information.

The subject of the commit message (i.e. first line) should contain a summary
of the changes. Please use the imperative mood. The subject can be prefixed
with "Test: " or "Docs: " to indicate the changes are not primarily to the main
code base. For example::

    Add DROP VIEW support to the planner and executor
    Test: Fix flakiness of JoinIntegrationTest
    Docs: Include ON CONFLICT clause in INSERT page

See also: https://chris.beams.io/posts/git-commit/


Updating Your Branch
--------------------

If new commits have been added to ``master`` since you created your feature
branch, please do not merge them in to your branch. Instead, rebase your branch::

    $ git fetch origin
    $ git rebase origin/master

This will apply all commits on your feature branch on top of the ``master``
branch. If there are conflicts, they can be resolved with ``git merge``.
After the conflict has been resolved, use ``git rebase --continue`` to
continue the rebase process.


Squashing Minor Commits
-----------------------

Minor commits that only fix typos or rename variables that are related to a
bigger change should be squashed into that commit.

This can be done with the following command::

    $ git rebase -i origin/master

This will open up a text editor where you can annotate your commits.

Generally, you'll want to leave the first commit listed as ``pick``, or
change it to ``reword`` (or ``r`` for short) if you want to change the commit
message. And then, if you want to squash every subsequent commit, you could
mark them all as ``fixup`` (or ``f`` for short).

Once you're done, you can check that it worked by running::

    $ git log

If you're happy with the result, do a **force** push (since you're rewriting history)::

    $ git push -f

See also: http://www.ericbmerritt.com/2011/09/21/commit-hygiene-and-git.html


.. _CLA: https://crate.io/community/contribute/cla/
.. _Crate.io: http://crate.io/
.. _developer guide: devs/docs/index.rst
.. _get in touch: https://crate.io/contact/
.. _giving feedback: https://github.com/crate/crate/blob/master/CONTRIBUTING.rst#tell-us-about-your-experience
.. _reporting new issues: https://github.com/crate/crate/blob/master/CONTRIBUTING.rst#reporting-issues
.. _search the existing issues: https://github.com/search?q=org%3Acrate+is%3Aissue+is%3Aopen
.. _sort issues by reaction: https://github.com/crate/crate/issues?q=is%3Aissue+is%3Aopen+sort%3Areactions-%2B1-desc
.. _support channels: https://crate.io/support/
.. _Upvoting existing issues: https://github.com/crate/crate/blob/master/CONTRIBUTING.rst#upvoting-issues
