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

package io.crate.operation.reference.sys.node.fs;

import io.crate.operation.reference.NestedObjectExpression;
import io.crate.operation.reference.sys.SysNodeObjectReference;
import io.crate.operation.reference.sys.SysObjectArrayReference;
import io.crate.monitor.ExtendedFsStats;
import org.apache.lucene.util.BytesRef;

import java.util.ArrayList;
import java.util.List;

public class NodeFsDisksExpression extends SysObjectArrayReference {

    private final ExtendedFsStats extendedFsStats;

    NodeFsDisksExpression(ExtendedFsStats extendedFsStats) {
        this.extendedFsStats = extendedFsStats;
    }

    @Override
    protected List<NestedObjectExpression> getChildImplementations() {
        List<NestedObjectExpression> diskRefs = new ArrayList<>(extendedFsStats.size());
        for (ExtendedFsStats.Info info : extendedFsStats) {
            diskRefs.add(new NodeFsDiskChildExpression(info));
        }
        return diskRefs;
    }

    private static class NodeFsDiskChildExpression extends SysNodeObjectReference {

        public static final String DEV = "dev";
        public static final String SIZE = "size";
        public static final String USED = "used";
        public static final String AVAILABLE = "available";
        public static final String READS = "reads";
        public static final String BYTES_READ = "bytes_read";
        public static final String WRITES = "writes";
        public static final String BYTES_WRITTEN = "bytes_written";

        private final ExtendedFsStats.Info fsInfo;

        protected NodeFsDiskChildExpression(ExtendedFsStats.Info fsInfo) {
            this.fsInfo = fsInfo;
            addChildImplementations();
        }

        private void addChildImplementations() {
            childImplementations.put(DEV, new ChildExpression<BytesRef>() {
                @Override
                public BytesRef value() {
                    return fsInfo.dev();
                }
            });
            childImplementations.put(SIZE, new ChildExpression<Long>() {
                @Override
                public Long value() {
                    return fsInfo.total() * 1024;
                }
            });
            childImplementations.put(USED, new ChildExpression<Long>() {
                @Override
                public Long value() {
                    return fsInfo.used() * 1024;
                }
            });
            childImplementations.put(AVAILABLE, new ChildExpression<Long>() {
                @Override
                public Long value() {
                    return fsInfo.available() * 1024;
                }
            });
            childImplementations.put(READS, new ChildExpression<Long>() {
                @Override
                public Long value() {
                    return fsInfo.diskReads();
                }
            });
            childImplementations.put(BYTES_READ, new ChildExpression<Long>() {
                @Override
                public Long value() {
                    return fsInfo.diskReadSizeInBytes();
                }
            });
            childImplementations.put(WRITES, new ChildExpression<Long>() {
                @Override
                public Long value() {
                    return fsInfo.diskWrites();
                }
            });
            childImplementations.put(BYTES_WRITTEN, new ChildExpression<Long>() {
                @Override
                public Long value() {
                    return fsInfo.diskWriteSizeInBytes();
                }
            });

        }

    }
}
