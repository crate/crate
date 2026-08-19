#!/usr/bin/env python3

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

""" shared plumbing of the release tools in this directory
"""

import re
import subprocess
import sys
from argparse import ArgumentParser
from pathlib import Path

VERSION_RE = re.compile(r"^\d+\.\d+\.\d+$")
NOTES_DIR = "docs/appendices/release-notes"
VERSION_JAVA = "server/src/main/java/org/elasticsearch/Version.java"


def run(*args, cwd, quiet=True):
    result = subprocess.run(
        args,
        cwd=cwd,
        text=True,
        stdout=subprocess.PIPE if quiet else None,
        stderr=subprocess.PIPE if quiet else None,
    )
    if result.returncode != 0:
        details = (result.stderr or result.stdout or "").strip()
        sys.exit(f"`{' '.join(args)}` failed" + (f": {details}" if details else ""))
    return (result.stdout or "").strip()


def version_arg(doc, help_text):
    """Parse the single ``version`` argument shared by the release tools"""
    parser = ArgumentParser(description=doc.strip().splitlines()[0])
    parser.add_argument("version", help=help_text)
    version = parser.parse_args().version
    if VERSION_RE.match(version) is None:
        sys.exit(f"invalid version '{version}', expected <major>.<minor>.<patch>")
    return version


def repo_root(script):
    """Root of the checkout the given script file lives in"""
    return Path(run("git", "rev-parse", "--show-toplevel", cwd=Path(script).resolve().parent))


def ref_exists(root, ref):
    return subprocess.run(
        ("git", "rev-parse", "--verify", "--quiet", ref),
        cwd=root,
        stdout=subprocess.DEVNULL,
    ).returncode == 0


def fetch_and_check(root, base, branch):
    """Verify the checkout is ready to create ``branch`` off ``origin/base``"""
    if run("git", "status", "--porcelain", cwd=root):
        sys.exit("working directory not clean, commit or stash your changes first")
    print("Fetching origin...")
    run("git", "fetch", "origin", cwd=root)
    if not ref_exists(root, f"refs/remotes/origin/{base}"):
        sys.exit(f"origin/{base} does not exist, is there a {base} release branch?")
    for ref in (f"refs/heads/{branch}", f"refs/remotes/origin/{branch}"):
        if ref_exists(root, ref):
            sys.exit(f"{ref} already exists, delete it or finish that release first")


def create_branch(root, branch, base):
    """Check out ``branch`` at ``origin/base``, returning the previous branch"""
    print(f"Creating branch {branch} from origin/{base}...")
    previous = run("git", "rev-parse", "--abbrev-ref", "HEAD", cwd=root)
    if previous == "HEAD":  # detached, remember the commit instead
        previous = run("git", "rev-parse", "HEAD", cwd=root)
    run("git", "checkout", "-b", branch, f"origin/{base}", cwd=root, quiet=False)
    return previous

def commit_and_push(root, branch, message):
    run("git", "add", "--all", cwd=root)
    run("git", "commit", "-m", message, cwd=root, quiet=False)
    print(f"Pushing {branch} to origin...")
    run("git", "push", "--set-upstream", "origin", branch, cwd=root, quiet=False)


def open_pull_request(root, base, branch, title):
    """Create the pull request of ``branch`` against ``base``"""
    print(f"Creating the pull request of {branch}...")
    run("gh", "pr", "create", "--base", base, "--head", branch,
        "--title", title, "--body", "", cwd=root, quiet=False)
