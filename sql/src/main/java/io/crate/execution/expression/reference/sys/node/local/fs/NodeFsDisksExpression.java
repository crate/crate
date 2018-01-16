/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.execution.expression.reference.sys.node.local.fs;

import io.crate.monitor.FsInfoHelpers;
import io.crate.execution.expression.reference.NestedObjectExpression;
import io.crate.execution.expression.reference.sys.SysObjectArrayReference;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.monitor.fs.FsInfo;

import java.util.ArrayList;
import java.util.List;

class NodeFsDisksExpression extends SysObjectArrayReference {

    private final FsInfo fsInfo;

    NodeFsDisksExpression(FsInfo fsInfo) {
        this.fsInfo = fsInfo;
    }

    @Override
    protected List<io.crate.execution.expression.reference.NestedObjectExpression> getChildImplementations() {
        List<io.crate.execution.expression.reference.NestedObjectExpression> diskRefs = new ArrayList<>();
        for (FsInfo.Path path : fsInfo) {
            diskRefs.add(new NodeFsDiskChildExpression(path));
        }
        return diskRefs;
    }

    private static class NodeFsDiskChildExpression extends NestedObjectExpression {

        private static final String DEV = "dev";
        private static final String SIZE = "size";
        private static final String USED = "used";
        private static final String AVAILABLE = "available";
        @Deprecated
        private static final String READS = "reads";
        @Deprecated
        private static final String BYTES_READ = "bytes_read";
        @Deprecated
        private static final String WRITES = "writes";
        @Deprecated
        private static final String BYTES_WRITTEN = "bytes_written";

        NodeFsDiskChildExpression(FsInfo.Path path) {
            addChildImplementations(path);
        }

        private void addChildImplementations(FsInfo.Path path) {
            childImplementations.put(DEV, () -> BytesRefs.toBytesRef(FsInfoHelpers.Path.dev(path)));
            childImplementations.put(SIZE, () -> FsInfoHelpers.Path.size(path));
            childImplementations.put(USED, () -> FsInfoHelpers.Path.used(path));
            childImplementations.put(AVAILABLE, () -> FsInfoHelpers.Path.available(path));
            childImplementations.put(READS, () -> -1L);
            childImplementations.put(BYTES_READ, () -> -1L);
            childImplementations.put(WRITES, () -> -1L);
            childImplementations.put(BYTES_WRITTEN, () -> -1L);
        }
    }
}
