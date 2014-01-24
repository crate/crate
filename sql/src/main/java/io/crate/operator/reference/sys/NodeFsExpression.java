/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed any another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.operator.reference.sys;

import com.google.common.collect.ImmutableList;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.sys.SysExpression;
import io.crate.metadata.sys.SystemReferences;
import org.cratedb.DataType;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.monitor.fs.FsStats;
import org.elasticsearch.node.service.NodeService;

public class NodeFsExpression extends SysObjectReference<Object> {

    abstract class FsExpression extends SysExpression<Object> {

        private final ReferenceInfo info;

        FsExpression(ReferenceInfo info) {
            this.info = info;
        }

        @Override
        public ReferenceInfo info() {
            return info;
        }
    }

    public static final String COLNAME = "fs";

    public static final String TOTAL = "total";
    public static final String FREE = "free";
    public static final String USED = "used";
    public static final String FREE_PERCENT = "free_percent";
    public static final String USED_PERCENT = "used_percent";

    public static final ReferenceInfo INFO_FS = SystemReferences.registerNodeReference(
            COLNAME, DataType.OBJECT);
    public static final ReferenceInfo INFO_FS_TOTAL = SystemReferences.registerNodeReference(
            COLNAME, DataType.LONG, ImmutableList.of(TOTAL));
    public static final ReferenceInfo INFO_FS_FREE = SystemReferences.registerNodeReference(
            COLNAME, DataType.LONG, ImmutableList.of(FREE));
    public static final ReferenceInfo INFO_FS_USED = SystemReferences.registerNodeReference(
            COLNAME, DataType.LONG, ImmutableList.of(USED));
    public static final ReferenceInfo INFO_FS_FREE_PERCENT = SystemReferences.registerNodeReference(
            COLNAME, DataType.DOUBLE, ImmutableList.of(FREE_PERCENT));
    public static final ReferenceInfo INFO_FS_USED_PERCENT = SystemReferences.registerNodeReference(
            COLNAME, DataType.DOUBLE, ImmutableList.of(USED_PERCENT));

    private final NodeService nodeService;

    @Inject
    public NodeFsExpression(NodeService nodeService) {
        this.nodeService = nodeService;
        addChildImplementations();
    }

    @Override
    public ReferenceInfo info() {
        return INFO_FS;
    }

    private void addChildImplementations() {
        childImplementations.put(TOTAL, new FsExpression(INFO_FS_TOTAL) {
            @Override
            public Long value() {
                return getTotalBytesFromAllDisks();
            }
        });
        childImplementations.put(FREE, new FsExpression(INFO_FS_FREE) {
            @Override
            public Long value() {
                return getFreeBytesFromAllDisks();
            }
        });
        childImplementations.put(USED, new FsExpression(INFO_FS_USED) {
            @Override
            public Long value() {
                return getTotalBytesFromAllDisks() - getFreeBytesFromAllDisks();
            }
        });
        childImplementations.put(FREE_PERCENT, new FsExpression(INFO_FS_FREE_PERCENT) {
            @Override
            public Double value() {
                return new Double((getFreeBytesFromAllDisks() / (double) getTotalBytesFromAllDisks())*100);
            }
        });
        childImplementations.put(USED_PERCENT, new FsExpression(INFO_FS_USED_PERCENT) {
            @Override
            public Double value() {
                Long total_bytes = getTotalBytesFromAllDisks();
                Long used_bytes = total_bytes - getFreeBytesFromAllDisks();
                return new Double((used_bytes / (double) total_bytes)*100);
            }
        });
    }

    private long getFreeBytesFromAllDisks() {
        Long bytes = 0L;
        for (FsStats.Info fsInfo : nodeService.stats().getFs()) {
            bytes += fsInfo.getFree().bytes();
        }
        return bytes;
    }

    private long getTotalBytesFromAllDisks() {
        Long bytes = 0L;
        for (FsStats.Info fsInfo : nodeService.stats().getFs()) {
            bytes += fsInfo.getTotal().bytes();
        }
        return bytes;
    }

}
