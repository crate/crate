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
import io.crate.metadata.expressions.RowCollectExpressionFactory;
import io.crate.metadata.sys.SysNodesTableInfo;
import io.crate.monitor.ExtendedFsStats;
import io.crate.monitor.ThreadPools;
import io.crate.operation.reference.sys.node.fs.*;
import org.apache.lucene.util.BytesRef;

import java.util.Map;

public class SysNodesExpressionFactories {

    private SysNodesExpressionFactories() {
    }

    private final static Map<ColumnIdent, RowCollectExpressionFactory<NodeStatsContext>> EXPRESSION_FACTORIES_BY_COLUMN =
        ImmutableMap.<ColumnIdent, RowCollectExpressionFactory<NodeStatsContext>>builder()

        .put(SysNodesTableInfo.Columns.ID, () -> new RowContextCollectorExpression<NodeStatsContext, BytesRef>() {
            @Override
            public BytesRef value() {
                return row.id();
            }
        })
        .put(SysNodesTableInfo.Columns.NAME, () -> new RowContextCollectorExpression<NodeStatsContext, BytesRef>() {
            @Override
            public BytesRef value() {
                return row.name();
            }
        })
        .put(SysNodesTableInfo.Columns.HOSTNAME, () -> new SimpleNodeStatsExpression<BytesRef>() {
            @Override
            public BytesRef innerValue() {
                return row.hostname();
            }
        })
        .put(SysNodesTableInfo.Columns.REST_URL, () -> new SimpleNodeStatsExpression<BytesRef>() {
            @Override
            public BytesRef innerValue() {
                return row.restUrl();
            }
        })
        .put(SysNodesTableInfo.Columns.PORT, NodePortStatsExpression::new)
        .put(SysNodesTableInfo.Columns.LOAD, NodeLoadStatsExpression::new)
        .put(SysNodesTableInfo.Columns.MEM, NodeMemoryStatsExpression::new)
        .put(SysNodesTableInfo.Columns.HEAP, NodeHeapStatsExpression::new)
        .put(SysNodesTableInfo.Columns.VERSION, NodeVersionStatsExpression::new)
        .put(SysNodesTableInfo.Columns.THREAD_POOLS, NodeThreadPoolsExpression::new)
        .put(SysNodesTableInfo.Columns.THREAD_POOLS_NAME, () -> new NodeStatsThreadPoolExpression<String>() {
            @Override
            protected String valueForItem(Map.Entry<String, ThreadPools.ThreadPoolExecutorContext> input) {
                return input.getKey();
            }
        })
        .put(SysNodesTableInfo.Columns.THREAD_POOLS_ACTIVE, () -> new NodeStatsThreadPoolExpression<Integer>() {
            @Override
            protected Integer valueForItem(Map.Entry<String, ThreadPools.ThreadPoolExecutorContext> input) {
                return input.getValue().activeCount();
            }
        })
        .put(SysNodesTableInfo.Columns.THREAD_POOLS_REJECTED, () -> new NodeStatsThreadPoolExpression<Long>() {
            @Override
            protected Long valueForItem(Map.Entry<String, ThreadPools.ThreadPoolExecutorContext> input) {
                return input.getValue().rejectedCount();
            }
        })
        .put(SysNodesTableInfo.Columns.THREAD_POOLS_LARGEST, () -> new NodeStatsThreadPoolExpression<Integer>() {
            @Override
            protected Integer valueForItem(Map.Entry<String, ThreadPools.ThreadPoolExecutorContext> input) {
                return input.getValue().largestPoolSize();
            }
        })
        .put(SysNodesTableInfo.Columns.THREAD_POOLS_COMPLETED, () -> new NodeStatsThreadPoolExpression<Long>() {
            @Override
            protected Long valueForItem(Map.Entry<String, ThreadPools.ThreadPoolExecutorContext> input) {
                return input.getValue().completedTaskCount();
            }
        })
        .put(SysNodesTableInfo.Columns.THREAD_POOLS_THREADS, () -> new NodeStatsThreadPoolExpression<Integer>() {
            @Override
            protected Integer valueForItem(Map.Entry<String, ThreadPools.ThreadPoolExecutorContext> input) {
                return input.getValue().poolSize();
            }
        })
        .put(SysNodesTableInfo.Columns.THREAD_POOLS_QUEUE, () -> new NodeStatsThreadPoolExpression<Integer>() {
            @Override
            protected Integer valueForItem(Map.Entry<String, ThreadPools.ThreadPoolExecutorContext> input) {
                return input.getValue().queueSize();
            }
        })
        .put(SysNodesTableInfo.Columns.NETWORK, NodeNetworkStatsExpression::new)
        .put(SysNodesTableInfo.Columns.OS, NodeOsStatsExpression::new)
        .put(SysNodesTableInfo.Columns.OS_INFO, NodeOsInfoStatsExpression::new)
        .put(SysNodesTableInfo.Columns.PROCESS, NodeProcessStatsExpression::new)
        .put(SysNodesTableInfo.Columns.FS, NodeFsStatsExpression::new)
        .put(SysNodesTableInfo.Columns.FS_TOTAL, NodeFsTotalStatsExpression::new)
        .put(SysNodesTableInfo.Columns.FS_TOTAL_SIZE, new RowCollectExpressionFactory<NodeStatsContext>() {
            @Override
            public RowCollectExpression<NodeStatsContext, ?> create() {
                return new SimpleNodeStatsExpression<Long>() {
                    @Override
                    public Long innerValue() {
                        return this.row.extendedFsStats().total().total();
                    }
                };
            }
        })
        .put(SysNodesTableInfo.Columns.FS_TOTAL_USED, new RowCollectExpressionFactory<NodeStatsContext>() {
            @Override
            public RowCollectExpression<NodeStatsContext, ?> create() {
                return new SimpleNodeStatsExpression<Long>() {
                    @Override
                    public Long innerValue() {
                        return this.row.extendedFsStats().total().used();
                    }
                };
            }
        })
        .put(SysNodesTableInfo.Columns.FS_TOTAL_AVAILABLE, new RowCollectExpressionFactory<NodeStatsContext>() {
            @Override
            public RowCollectExpression<NodeStatsContext, ?> create() {
                return new SimpleNodeStatsExpression<Long>() {
                    @Override
                    public Long innerValue() {
                        return this.row.extendedFsStats().total().available();
                    }
                };
            }
        })
        .put(SysNodesTableInfo.Columns.FS_TOTAL_READS, new RowCollectExpressionFactory<NodeStatsContext>() {
            @Override
            public RowCollectExpression<NodeStatsContext, ?> create() {
                return new SimpleNodeStatsExpression<Long>() {
                    @Override
                    public Long innerValue() {
                        return this.row.extendedFsStats().total().diskReads();
                    }
                };
            }
        })
        .put(SysNodesTableInfo.Columns.FS_TOTAL_BYTES_READ, new RowCollectExpressionFactory<NodeStatsContext>() {
            @Override
            public RowCollectExpression<NodeStatsContext, ?> create() {
                return new SimpleNodeStatsExpression<Long>() {
                    @Override
                    public Long innerValue() {
                        return this.row.extendedFsStats().total().diskReadSizeInBytes();
                    }
                };
            }
        })
        .put(SysNodesTableInfo.Columns.FS_TOTAL_WRITES, new RowCollectExpressionFactory<NodeStatsContext>() {
            @Override
            public RowCollectExpression<NodeStatsContext, ?> create() {
                return new SimpleNodeStatsExpression<Long>() {
                    @Override
                    public Long innerValue() {
                        return this.row.extendedFsStats().total().diskWrites();
                    }
                };
            }
        })
        .put(SysNodesTableInfo.Columns.FS_TOTAL_BYTES_WRITTEN, new RowCollectExpressionFactory<NodeStatsContext>() {
            @Override
            public RowCollectExpression<NodeStatsContext, ?> create() {
                return new SimpleNodeStatsExpression<Long>() {
                    @Override
                    public Long innerValue() {
                        return this.row.extendedFsStats().total().diskWriteSizeInBytes();
                    }
                };
            }
        })
        .put(SysNodesTableInfo.Columns.FS_DISKS, NodeStatsFsDisksExpression::new)
        .put(SysNodesTableInfo.Columns.FS_DISKS_DEV, () -> new NodeStatsFsArrayExpression<BytesRef>() {
            @Override
            protected BytesRef valueForItem(ExtendedFsStats.Info input) {
                return input.dev();
            }
        })
        .put(SysNodesTableInfo.Columns.FS_DISKS_SIZE, () -> new NodeStatsFsArrayExpression<Long>() {
            @Override
            protected Long valueForItem(ExtendedFsStats.Info input) {
                return input.total();
            }
        })
        .put(SysNodesTableInfo.Columns.FS_DISKS_USED, () -> new NodeStatsFsArrayExpression<Long>() {
            @Override
            protected Long valueForItem(ExtendedFsStats.Info input) {
                return input.used();
            }
        })
        .put(SysNodesTableInfo.Columns.FS_DISKS_AVAILABLE, () -> new NodeStatsFsArrayExpression<Long>() {
            @Override
            protected Long valueForItem(ExtendedFsStats.Info input) {
                return input.available();
            }
        })
        .put(SysNodesTableInfo.Columns.FS_DISKS_READS, () -> new NodeStatsFsArrayExpression<Long>() {
            @Override
            protected Long valueForItem(ExtendedFsStats.Info input) {
                return input.diskReads();
            }
        })
        .put(SysNodesTableInfo.Columns.FS_DISKS_BYTES_READ, () -> new NodeStatsFsArrayExpression<Long>() {
            @Override
            protected Long valueForItem(ExtendedFsStats.Info input) {
                return input.diskReadSizeInBytes();
            }
        })
        .put(SysNodesTableInfo.Columns.FS_DISKS_WRITES, () -> new NodeStatsFsArrayExpression<Long>() {
            @Override
            protected Long valueForItem(ExtendedFsStats.Info input) {
                return input.diskWrites();
            }
        })
        .put(SysNodesTableInfo.Columns.FS_DISKS_BYTES_WRITTEN, () -> new NodeStatsFsArrayExpression<Long>() {
            @Override
            protected Long valueForItem(ExtendedFsStats.Info input) {
                return input.diskWriteSizeInBytes();
            }
        })
        .put(SysNodesTableInfo.Columns.FS_DATA, NodeStatsFsDataExpression::new)
        .put(SysNodesTableInfo.Columns.FS_DATA_DEV, () -> new NodeStatsFsArrayExpression<BytesRef>() {
            @Override
            protected BytesRef valueForItem(ExtendedFsStats.Info input) {
                return input.dev();
            }
        })
        .put(SysNodesTableInfo.Columns.FS_DATA_PATH, () -> new NodeStatsFsArrayExpression<BytesRef>() {
            @Override
            protected BytesRef valueForItem(ExtendedFsStats.Info input) {
                return input.path();
            }
        })
        .build();

    public static Map<ColumnIdent, RowCollectExpressionFactory<NodeStatsContext>> getNodeStatsContextExpressions() {
        return EXPRESSION_FACTORIES_BY_COLUMN;
    }
}
