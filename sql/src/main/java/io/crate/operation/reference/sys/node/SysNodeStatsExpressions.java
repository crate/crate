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

package io.crate.operation.reference.sys.node;

import com.google.common.collect.ImmutableMap;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RowCollectExpression;
import io.crate.metadata.RowContextCollectorExpression;
import io.crate.metadata.sys.SysNodesTableInfo;
import io.crate.monitor.ExtendedFsStats;
import io.crate.monitor.ThreadPools;
import io.crate.operation.reference.sys.node.fs.*;
import org.apache.lucene.util.BytesRef;

import java.util.Map;

public class SysNodeStatsExpressions {

    private static Map<ColumnIdent, RowCollectExpression> expressions = ImmutableMap.<ColumnIdent, RowCollectExpression>builder()
        .put(SysNodesTableInfo.Columns.ID, new RowContextCollectorExpression<NodeStatsContext, BytesRef>() {
            @Override
            public BytesRef value() {
                return row.id();
            }
        })
        .put(SysNodesTableInfo.Columns.NAME, new RowContextCollectorExpression<NodeStatsContext, BytesRef>() {
            @Override
            public BytesRef value() {
                return row.name();
            }
        })
        .put(SysNodesTableInfo.Columns.HOSTNAME, new SimpleNodeStatsExpression<BytesRef>() {
            @Override
            public BytesRef innerValue() {
                return row.hostname();
            }
        })
        .put(SysNodesTableInfo.Columns.REST_URL, new SimpleNodeStatsExpression<BytesRef>() {
            @Override
            public BytesRef innerValue() {
                return row.restUrl();
            }
        })
        .put(SysNodesTableInfo.Columns.PORT, new NodePortStatsExpression())
        .put(SysNodesTableInfo.Columns.LOAD, new NodeLoadStatsExpression())
        .put(SysNodesTableInfo.Columns.MEM, new NodeMemoryStatsExpression())
        .put(SysNodesTableInfo.Columns.HEAP, new NodeHeapStatsExpression())
        .put(SysNodesTableInfo.Columns.VERSION, new NodeVersionStatsExpression())
        .put(SysNodesTableInfo.Columns.THREAD_POOLS, new NodeThreadPoolsExpression())
        .put(SysNodesTableInfo.Columns.THREAD_POOLS_NAME, new NodeStatsThreadPoolExpression<String>() {
            @Override
            protected String valueForItem(Map.Entry<String, ThreadPools.ThreadPoolExecutorContext> input) {
                return input.getKey();
            }
        })
        .put(SysNodesTableInfo.Columns.THREAD_POOLS_ACTIVE, new NodeStatsThreadPoolExpression<Integer>() {
            @Override
            protected Integer valueForItem(Map.Entry<String, ThreadPools.ThreadPoolExecutorContext> input) {
                return input.getValue().activeCount();
            }
        })
        .put(SysNodesTableInfo.Columns.THREAD_POOLS_REJECTED, new NodeStatsThreadPoolExpression<Long>() {
            @Override
            protected Long valueForItem(Map.Entry<String, ThreadPools.ThreadPoolExecutorContext> input) {
                return input.getValue().rejectedCount();
            }
        })
        .put(SysNodesTableInfo.Columns.THREAD_POOLS_LARGEST, new NodeStatsThreadPoolExpression<Integer>() {
            @Override
            protected Integer valueForItem(Map.Entry<String, ThreadPools.ThreadPoolExecutorContext> input) {
                return input.getValue().largestPoolSize();
            }
        })
        .put(SysNodesTableInfo.Columns.THREAD_POOLS_COMPLETED, new NodeStatsThreadPoolExpression<Long>() {
            @Override
            protected Long valueForItem(Map.Entry<String, ThreadPools.ThreadPoolExecutorContext> input) {
                return input.getValue().completedTaskCount();
            }
        })
        .put(SysNodesTableInfo.Columns.THREAD_POOLS_THREADS, new NodeStatsThreadPoolExpression<Integer>() {
            @Override
            protected Integer valueForItem(Map.Entry<String, ThreadPools.ThreadPoolExecutorContext> input) {
                return input.getValue().poolSize();
            }
        })
        .put(SysNodesTableInfo.Columns.THREAD_POOLS_QUEUE, new NodeStatsThreadPoolExpression<Integer>() {
            @Override
            protected Integer valueForItem(Map.Entry<String, ThreadPools.ThreadPoolExecutorContext> input) {
                return input.getValue().queueSize();
            }
        })
        .put(SysNodesTableInfo.Columns.NETWORK, new NodeNetworkStatsExpression())
        .put(SysNodesTableInfo.Columns.OS, new NodeOsStatsExpression())
        .put(SysNodesTableInfo.Columns.OS_INFO, new NodeOsInfoStatsExpression())
        .put(SysNodesTableInfo.Columns.PROCESS, new NodeProcessStatsExpression())
        .put(SysNodesTableInfo.Columns.FS, new NodeFsStatsExpression())
        .put(SysNodesTableInfo.Columns.FS_TOTAL, new NodeFsTotalStatsExpression())
        .put(SysNodesTableInfo.Columns.FS_TOTAL_SIZE, new SimpleNodeStatsExpression<Long>() {
            @Override
            public Long innerValue() {
                return this.row.extendedFsStats().total().total();
            }
        })
        .put(SysNodesTableInfo.Columns.FS_TOTAL_USED, new SimpleNodeStatsExpression<Long>() {
            @Override
            public Long innerValue() {
                return this.row.extendedFsStats().total().used();
            }
        })
        .put(SysNodesTableInfo.Columns.FS_TOTAL_AVAILABLE, new SimpleNodeStatsExpression<Long>() {
            @Override
            public Long innerValue() {
                return this.row.extendedFsStats().total().available();
            }
        })
        .put(SysNodesTableInfo.Columns.FS_TOTAL_READS, new SimpleNodeStatsExpression<Long>() {
            @Override
            public Long innerValue() {
                return this.row.extendedFsStats().total().diskReads();
            }
        })
        .put(SysNodesTableInfo.Columns.FS_TOTAL_BYTES_READ, new SimpleNodeStatsExpression<Long>() {
            @Override
            public Long innerValue() {
                return this.row.extendedFsStats().total().diskReadSizeInBytes();
            }
        })
        .put(SysNodesTableInfo.Columns.FS_TOTAL_WRITES, new SimpleNodeStatsExpression<Long>() {
            @Override
            public Long innerValue() {
                return this.row.extendedFsStats().total().diskWrites();
            }
        })
        .put(SysNodesTableInfo.Columns.FS_TOTAL_BYTES_WRITTEN, new SimpleNodeStatsExpression<Long>() {
            @Override
            public Long innerValue() {
                return this.row.extendedFsStats().total().diskWriteSizeInBytes();
            }
        })
        .put(SysNodesTableInfo.Columns.FS_DISKS, new NodeStatsFsDisksExpression())
        .put(SysNodesTableInfo.Columns.FS_DISKS_DEV, new NodeStatsFsArrayExpression<BytesRef>() {
            @Override
            protected BytesRef valueForItem(ExtendedFsStats.Info input) {
                return input.dev();
            }
        })
        .put(SysNodesTableInfo.Columns.FS_DISKS_SIZE, new NodeStatsFsArrayExpression<Long>() {
            @Override
            protected Long valueForItem(ExtendedFsStats.Info input) {
                return input.total();
            }
        })
        .put(SysNodesTableInfo.Columns.FS_DISKS_USED, new NodeStatsFsArrayExpression<Long>() {
            @Override
            protected Long valueForItem(ExtendedFsStats.Info input) {
                return input.used();
            }
        })
        .put(SysNodesTableInfo.Columns.FS_DISKS_AVAILABLE, new NodeStatsFsArrayExpression<Long>() {
            @Override
            protected Long valueForItem(ExtendedFsStats.Info input) {
                return input.available();
            }
        })
        .put(SysNodesTableInfo.Columns.FS_DISKS_READS, new NodeStatsFsArrayExpression<Long>() {
            @Override
            protected Long valueForItem(ExtendedFsStats.Info input) {
                return input.diskReads();
            }
        })
        .put(SysNodesTableInfo.Columns.FS_DISKS_BYTES_READ, new NodeStatsFsArrayExpression<Long>() {
            @Override
            protected Long valueForItem(ExtendedFsStats.Info input) {
                return input.diskReadSizeInBytes();
            }
        })
        .put(SysNodesTableInfo.Columns.FS_DISKS_WRITES, new NodeStatsFsArrayExpression<Long>() {
            @Override
            protected Long valueForItem(ExtendedFsStats.Info input) {
                return input.diskWrites();
            }
        })
        .put(SysNodesTableInfo.Columns.FS_DISKS_BYTES_WRITTEN, new NodeStatsFsArrayExpression<Long>() {
            @Override
            protected Long valueForItem(ExtendedFsStats.Info input) {
                return input.diskWriteSizeInBytes();
            }
        })
        .put(SysNodesTableInfo.Columns.FS_DATA, new NodeStatsFsDataExpression())
        .put(SysNodesTableInfo.Columns.FS_DATA_DEV, new NodeStatsFsArrayExpression<BytesRef>() {
            @Override
            protected BytesRef valueForItem(ExtendedFsStats.Info input) {
                return input.dev();
            }
        })
        .put(SysNodesTableInfo.Columns.FS_DATA_PATH, new NodeStatsFsArrayExpression<BytesRef>() {
            @Override
            protected BytesRef valueForItem(ExtendedFsStats.Info input) {
                return input.path();
            }
        })
        .build();

    public static Map<ColumnIdent, RowCollectExpression> getSysNodeStatsExpressions() {
        return expressions;
    }
}
