#!/bin/bash


# Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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


# check if everything is committed
CLEAN=`git status -s`
if [ ! -z "$CLEAN" ]
then
   echo "Working directory not clean. Please commit all changes before tagging"
   echo "Aborting."
   exit -1
fi

echo "Fetching origin..."
git fetch origin > /dev/null

# check if current branch is master
BRANCH=`git branch | grep "^*" | cut -d " " -f 2`
if [ "$BRANCH" != "master" ]
then
   echo "Current branch is $BRANCH. Must be master."
   echo "Aborting."
   exit -1
fi

# check if master == origin/master
MASTER_COMMIT=`git show --format="%H" master`
ORIGINMASTER_COMMIT=`git show --format="%H" origin/master`

if [ "$MASTER_COMMIT" != "$ORIGINMASTER_COMMIT" ]
then
   echo "Local master is not up to date. "
   echo "Aborting."
   exit -1
fi

# install locally so we can get the version
./gradlew install
# get the version
VERSION=`./app/build/install/crate/bin/crate -v | cut -d " " -f 2 | tr -d ','`

# check if tag to create has already been created
EXISTS=`git tag | grep $VERSION`

if [ "$VERSION" == "$EXISTS" ]
then
   echo "Revision $VERSION already tagged."
   echo "Aborting."
   exit -1
fi

# check if VERSION is in head of CHANGES.txt
REV_NOTE=`grep "[0-9/]\{10\} $VERSION" CHANGES.txt`
if [ -z "$REV_NOTE" ]
then
    echo "No notes for revision $VERSION found in CHANGES.txt"
    echo "Aborting."
    exit -1
fi

echo "Creating tag $VERSION..."
git tag -a "$VERSION" -m "Tag release for revision $VERSION"
git push --tags
echo "Done."
