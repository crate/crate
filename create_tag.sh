#!/bin/bash

# Shared Source Software
# Copyright (c) 2006-2013, Lovely Systems GmbH


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

# check if tag to create has already been created
VERSION=`mvn validate | grep "Building crate" | cut -d " " -f 4`
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
