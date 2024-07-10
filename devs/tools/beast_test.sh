#!/bin/bash

#
# Licensed to Crate.io GmbH ("Crate") under one or more contributor
# license agreements.  See the NOTICE file distributed with this work for
# additional information regarding copyright ownership.  Crate licenses
# this file to you under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.  You may
# obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
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
#

# Info: Be aware from where you are invoking this script due to relative paths.
#       For example, 'mvnw' has to be in the same directory as where you are
#       invoking this script from.
#       E.g.: devs/tools/beast_test.sh

THREADS=$1
TEST=$2
for i in $(seq 1 $THREADS); do
  ./mvnw test -pl server -Dtest="$TEST" -Dtests.iters=20 "${@:3}" &
done
wait
