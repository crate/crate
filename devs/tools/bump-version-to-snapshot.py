#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Licensed to Crate.io GmbH ("Crate") under one or more contributor
# license agreements.  See the NOTICE file distributed with this work for
# additional information regarding copyright ownership.  Crate licenses
# this file to you under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.  You may
# obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
# License for the specific language governing permissions and limitations
# under the License.
#
# However, if you have executed another commercial license agreement
# with Crate these terms will supersede the license and you may use the
# software solely pursuant to the terms of the relevant commercial agreement.

""" script to prepare the "Bump version to <version>-SNAPSHOT" commit

Given the next patch version like 5.5.1 it creates a ``bump-5.5.1`` branch off
``origin/5.5``, containing a single "Bump version to 5.5.1-SNAPSHOT" commit
which:

- sets the version in all ``pom.xml`` files to 5.5.1 by running
  ``./mvnw versions:set``

- adds a ``V_5_5_1`` constant with the snapshot flag set to
  ``server/src/main/java/org/elasticsearch/Version.java`` and makes it
  ``CURRENT``

- adds an "Unreleased" ``docs/appendices/release-notes/5.5.1.rst`` and lists it
  in ``docs/appendices/release-notes/index.rst``

- updates the version of the reindex example in
  ``docs/admin/system-information.rst``

The branch is pushed to origin and a link to open the pull request is printed.

Usage::

    ./devs/tools/bump-version-to-snapshot.py 5.5.1
"""

import re
from textwrap import fill

from release_helpers import (NOTES_DIR, VERSION_JAVA, commit_and_push, create_branch,
                            discard_branch, fail, fetch_and_check, print_pull_request_link,
                            repo_root, run, version_arg)

INDEX_RST = f"{NOTES_DIR}/index.rst"
SYSTEM_INFORMATION_RST = "docs/admin/system-information.rst"
DOCS_URL = "https://cratedb.com/docs/crate/reference/en/latest"

NOTES_TEMPLATE = """\
.. _version_{version}:

{marker}
Version {version} - Unreleased
{marker}

.. comment 1. Remove the " - Unreleased" from the header above and adjust the ==
.. comment 2. Remove the NOTE below and replace with: "Released on 20XX-XX-XX."
.. comment    (without a NOTE entry, simply starting from col 1 of the line)
.. NOTE::

    In development. {version} isn't released yet. These are the release notes for
    the upcoming release.

.. NOTE::

    If you are upgrading a cluster, you must be running CrateDB {minimum} or higher
    before you upgrade to {version}.

    We recommend that you upgrade to the latest {previous_series} release before moving to
    {version}.

{rolling_upgrade}
    Before upgrading, you should `back up your data`_.

.. WARNING::

    Tables that were created before CrateDB {previous_major}.x will not function with {major}.x
    and must be recreated before moving to {major}.x.x.

    You can recreate tables using ``COPY TO`` and ``COPY FROM`` or by
    `inserting the data into a new table`_.

.. _back up your data: {docs_url}/admin/snapshots.html
.. _inserting the data into a new table: {docs_url}/admin/system-information.html#tables-need-to-be-recreated

.. rubric:: Table of contents

.. contents::
   :local:


{series_reference}

Fixes
=====

None
"""


def render_release_notes(version, previous_notes):
    """Render the "Unreleased" release notes of ``version``

    The upgrade requirements are not derivable from the version, they are taken
    from ``previous_notes``, the notes of the previous patch version of the same
    series.
    """
    major, minor, _ = version.split(".")
    series = f"{major}.{minor}"

    def extract(name, pattern):
        match = re.search(pattern, previous_notes, re.MULTILINE | re.DOTALL)
        if match is None:
            raise ValueError(f"cannot tell the {name} from the previous release notes")
        return match

    minimum = extract("minimum version", r"you must be running CrateDB (\S+) or higher").group(1)
    previous_series = extract("previous series", r"upgrade to the latest (\S+) release").group(1)
    # the whole rolling upgrade paragraph is carried over, only the version it
    # upgrades to changes; some series add sentences about upgrade restrictions
    rolling_upgrade = extract("rolling upgrade support",
                              r"^( *A rolling upgrade from .+? to )\S+( is supported\..*?)"
                              r"(?=\n *Before upgrading)").expand(rf"\1{version}\2")

    title = f"Version {version} - Unreleased"
    reference = (f"See the :ref:`version_{series}.0` release notes for a full list of "
                 f"changes in the {series} series.")
    return NOTES_TEMPLATE.format(
        version=version,
        marker="=" * len(title),
        major=major,
        previous_major=int(major) - 1,
        docs_url=DOCS_URL,
        series_reference=fill(reference, width=80),
        **values,
    )


def patch_index(text, version, previous):
    """List ``version`` above ``previous`` in the release notes index"""
    entry = f"    {version}\n"
    if entry in text:
        raise ValueError(f"{version} is already listed in {INDEX_RST}")
    previous_entry = f"    {previous}\n"
    if previous_entry not in text:
        raise ValueError(f"{previous} is not listed in {INDEX_RST}")
    return text.replace(previous_entry, entry + previous_entry, 1)


def patch_system_information(text, version, previous):
    """Update the version of the reindex example, keeping the table aligned"""
    cell = re.compile(r"^(\s*\| )(\d+\.\d+\.\d+)( +)\|$", re.MULTILINE)
    matches = cell.findall(text)
    if len(matches) != 1:
        raise ValueError(f"expected one version cell in {SYSTEM_INFORMATION_RST}, "
                         f"found {len(matches)}")
    _, found, padding = matches[0]
    if found != previous:
        raise ValueError(f"the reindex example in {SYSTEM_INFORMATION_RST} shows {found}, "
                         f"not {previous}")
    width = len(found) + len(padding)
    return cell.sub(lambda m: f"{m.group(1)}{version.ljust(width)}|", text, count=1)


def add_version_constant(text, version, previous):
    """Add a snapshot ``V_<version>`` constant and make it ``CURRENT``"""
    constant = "V_" + version.replace(".", "_")
    previous_constant = "V_" + previous.replace(".", "_")
    if re.search(rf"^\s*public static final Version {constant} = ", text, re.MULTILINE):
        raise ValueError(f"{constant} already exists in {VERSION_JAVA}")

    declaration = re.compile(
        r"^\s*public static final Version " + previous_constant
        + r" = new Version\((?P<id>\w+), (?P<snapshot>true|false), (?P<lucene>.+)\);$",
        re.MULTILINE)
    match = declaration.search(text)
    if match is None:
        raise ValueError(f"no {previous_constant} constant in {VERSION_JAVA}")
    if match.group("snapshot") == "true":
        raise ValueError(f"{previous_constant} in {VERSION_JAVA} is still a snapshot, "
                         f"release {previous} first")

    # ids look like 9_04_01_99, only the patch part changes
    id_parts = match.group("id").split("_")
    id_parts[2] = f"{int(version.split('.')[2]):02d}"
    added = (f"    public static final Version {constant} = "
             f"new Version({'_'.join(id_parts)}, true, {match.group('lucene')});")
    text = f"{text[:match.end()]}\n{added}{text[match.end():]}"

    current = re.compile(
        r"^(\s*public static final Version CURRENT = )" + previous_constant + r";$",
        re.MULTILINE)
    if current.search(text) is None:
        raise ValueError(f"CURRENT is not {previous_constant} in {VERSION_JAVA}")
    return current.sub(rf"\g<1>{constant};", text, count=1)


def set_pom_version(root, version, previous):
    """Set the version in all pom.xml files, using the maven versions plugin"""
    pom = (root / "pom.xml").read_text()
    match = re.search(r"^    <version>(\S+)</version>$", pom, re.MULTILINE)
    if match is None:
        raise ValueError("cannot find the project version in pom.xml")
    if match.group(1) != previous:
        raise ValueError(f"pom.xml is at {match.group(1)}, not at {previous}")

    print(f"Setting the pom.xml version to {version}...")
    run("./mvnw", "--quiet", "versions:set", f"-DnewVersion={version}",
        "-DgenerateBackupPoms=false", cwd=root, quiet=False)
    changed = [line[3:] for line in
               run("git", "status", "--porcelain", cwd=root).splitlines()]
    unexpected = [path for path in changed if not path.endswith("pom.xml")]
    if unexpected:
        raise ValueError(f"versions:set changed unexpected files: {', '.join(unexpected)}")
    if not changed:
        raise ValueError("versions:set did not change any pom.xml")
    return changed


def main():
    version = version_arg(__doc__, "version to bump to, e.g. 5.5.1")
    major, minor, patch = (int(part) for part in version.split("."))
    if patch == 0:
        fail(f"{version} is not a patch version, bump to x.y.0 versions manually")
    base = f"{major}.{minor}"
    previous = f"{base}.{patch - 1}"
    branch = f"bump-{version}"

    root = repo_root(__file__)
    fetch_and_check(root, base, branch)
    previous_branch = create_branch(root, branch, base)

    notes = f"{NOTES_DIR}/{version}.rst"
    created = (notes,)
    try:
        if (root / notes).is_file():
            raise ValueError(f"{notes} already exists")
        previous_notes = root / f"{NOTES_DIR}/{previous}.rst"
        if not previous_notes.is_file():
            raise ValueError(f"{previous_notes.name} does not exist on origin/{base}")

        updates = {notes: render_release_notes(version, previous_notes.read_text())}
        for path, patch_file in (
            (INDEX_RST, patch_index),
            (SYSTEM_INFORMATION_RST, patch_system_information),
            (VERSION_JAVA, add_version_constant),
        ):
            updates[path] = patch_file((root / path).read_text(), version, previous)

        # the poms go first, so that versions:set is the only thing that has
        # touched the checkout when its changes are inspected
        poms = set_pom_version(root, version, previous)
        for path, content in updates.items():
            (root / path).write_text(content)
            print(f"Updated {path}")
    except ValueError as e:
        discard_branch(root, branch, previous_branch, created)
        fail(str(e))

    commit_and_push(root, branch, f"Bump version to {version}-SNAPSHOT",
                    tuple(updates) + tuple(poms))
    print_pull_request_link(root, base, branch, f"Bump version to {version}-SNAPSHOT")


if __name__ == "__main__":
    main()
