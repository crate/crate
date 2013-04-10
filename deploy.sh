#!/bin/bash

VERSION=`mvn validate | grep "Building crate" | cut -d " " -f 4`

mvn package

echo "uploading crate-${VERSION}.tar.gz"
ssh bs.ls.af "sudo tee /mnt/data1/www/download.lovelysystems.com/lovely/crate-${VERSION}.tar.gz > /dev/null" < "releases/crate-${VERSION}.tar.gz"