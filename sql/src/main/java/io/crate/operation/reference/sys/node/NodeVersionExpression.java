/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.operation.reference.sys.node;

import io.crate.Build;
import io.crate.Version;
import io.crate.metadata.ColumnIdent;
import io.crate.operation.reference.sys.SysNodeObjectReference;

public class NodeVersionExpression extends SysNodeObjectReference {

    public static final String NAME = "version";
    public static final String NUMBER = "number";
    public static final String BUILD_HASH = "build_hash";
    public static final String BUILD_SNAPSHOT = "build_snapshot";

    protected NodeVersionExpression() {
        super(NAME);
        childImplementations.put(NUMBER, new VersionNumberExpression());
        childImplementations.put(BUILD_HASH, new VersionBuildHashExpression());
        childImplementations.put(BUILD_SNAPSHOT, new VersionBuildSnapshotExpression());
    }

    static class VersionNumberExpression extends SysNodeExpression<String> {

        private final String versionNumber;

        VersionNumberExpression() {
            super(new ColumnIdent(NAME, NUMBER));
            versionNumber = Version.CURRENT.number();
        }

        @Override
        public String value() {
            return versionNumber;
        }
    }

    static class VersionBuildHashExpression extends SysNodeExpression<String> {

        private final String buildHash;

        VersionBuildHashExpression() {
            super(new ColumnIdent(NAME, BUILD_HASH));
            buildHash = Build.CURRENT.hash();
        }

        @Override
        public String value() {
            return buildHash;
        }
    }

    static class VersionBuildSnapshotExpression extends SysNodeExpression<Boolean> {

        VersionBuildSnapshotExpression() {
            super(new ColumnIdent(NAME, BUILD_SNAPSHOT));
        }

        @Override
        public Boolean value() {
            return Version.CURRENT.snapshot;
        }
    }

}
