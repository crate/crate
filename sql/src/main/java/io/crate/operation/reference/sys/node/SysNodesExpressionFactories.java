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

    public SysNodesExpressionFactories() {
    }

    public static Map<ColumnIdent, RowCollectExpressionFactory> getSysNodesTableInfoFactories() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory>builder()
            .put(SysNodesTableInfo.Columns.ID, new RowCollectExpressionFactory() {
                @Override
                public RowCollectExpression create() {
                    return new RowContextCollectorExpression<NodeStatsContext, BytesRef>() {
                        @Override
                        public BytesRef value() {
                            return row.id();
                        }
                    };
                }
            })
            .put(SysNodesTableInfo.Columns.NAME, new RowCollectExpressionFactory() {
                @Override
                public RowCollectExpression create() {
                    return new RowContextCollectorExpression<NodeStatsContext, BytesRef>() {
                        @Override
                        public BytesRef value() {
                            return row.name();
                        }
                    };
                }
            })
            .put(SysNodesTableInfo.Columns.HOSTNAME, new RowCollectExpressionFactory() {
                @Override
                public RowCollectExpression create() {
                    return new SimpleNodeStatsExpression<BytesRef>() {
                        @Override
                        public BytesRef innerValue() {
                            return row.hostname();
                        }
                    };
                }
            })
            .put(SysNodesTableInfo.Columns.REST_URL, new RowCollectExpressionFactory() {
                @Override
                public RowCollectExpression create() {
                    return new SimpleNodeStatsExpression<BytesRef>() {
                        @Override
                        public BytesRef innerValue() {
                            return row.restUrl();
                        }
                    };
                }
            })
            .put(SysNodesTableInfo.Columns.PORT, new RowCollectExpressionFactory() {
                @Override
                public RowCollectExpression create() {
                    return new NodePortStatsExpression();
                }
            })
            .put(SysNodesTableInfo.Columns.LOAD, new RowCollectExpressionFactory() {
                @Override
                public RowCollectExpression create() {
                    return new NodeLoadStatsExpression();
                }
            })
            .put(SysNodesTableInfo.Columns.MEM, new RowCollectExpressionFactory() {
                @Override
                public RowCollectExpression create() {
                    return new NodeMemoryStatsExpression();
                }
            })
            .put(SysNodesTableInfo.Columns.HEAP, new RowCollectExpressionFactory() {
                @Override
                public RowCollectExpression create() {
                    return new NodeHeapStatsExpression();
                }
            })
            .put(SysNodesTableInfo.Columns.VERSION, new RowCollectExpressionFactory() {
                @Override
                public RowCollectExpression create() {
                    return new NodeVersionStatsExpression();
                }
            })
            .put(SysNodesTableInfo.Columns.THREAD_POOLS, new RowCollectExpressionFactory() {
                @Override
                public RowCollectExpression create() {
                    return new NodeThreadPoolsExpression();
                }
            })
            .put(SysNodesTableInfo.Columns.THREAD_POOLS_NAME, new RowCollectExpressionFactory() {
                @Override
                public RowCollectExpression create() {
                    return new NodeStatsThreadPoolExpression<String>() {
                        @Override
                        protected String valueForItem(Map.Entry<String, ThreadPools.ThreadPoolExecutorContext> input) {
                            return input.getKey();
                        }
                    };
                }
            })
            .put(SysNodesTableInfo.Columns.THREAD_POOLS_ACTIVE, new RowCollectExpressionFactory() {
                @Override
                public RowCollectExpression create() {
                    return new NodeStatsThreadPoolExpression<Integer>() {
                        @Override
                        protected Integer valueForItem(Map.Entry<String, ThreadPools.ThreadPoolExecutorContext> input) {
                            return input.getValue().activeCount();
                        }
                    };
                }
            })
            .put(SysNodesTableInfo.Columns.THREAD_POOLS_REJECTED, new RowCollectExpressionFactory() {
                @Override
                public RowCollectExpression create() {
                    return new NodeStatsThreadPoolExpression<Long>() {
                        @Override
                        protected Long valueForItem(Map.Entry<String, ThreadPools.ThreadPoolExecutorContext> input) {
                            return input.getValue().rejectedCount();
                        }
                    };
                }
            })
            .put(SysNodesTableInfo.Columns.THREAD_POOLS_LARGEST, new RowCollectExpressionFactory() {
                @Override
                public RowCollectExpression create() {
                    return new NodeStatsThreadPoolExpression<Integer>() {
                        @Override
                        protected Integer valueForItem(Map.Entry<String, ThreadPools.ThreadPoolExecutorContext> input) {
                            return input.getValue().largestPoolSize();
                        }
                    };
                }
            })
            .put(SysNodesTableInfo.Columns.THREAD_POOLS_COMPLETED, new RowCollectExpressionFactory() {
                @Override
                public RowCollectExpression create() {
                    return new NodeStatsThreadPoolExpression<Long>() {
                        @Override
                        protected Long valueForItem(Map.Entry<String, ThreadPools.ThreadPoolExecutorContext> input) {
                            return input.getValue().completedTaskCount();
                        }
                    };
                }
            })
            .put(SysNodesTableInfo.Columns.THREAD_POOLS_THREADS, new RowCollectExpressionFactory() {
                @Override
                public RowCollectExpression create() {
                    return new NodeStatsThreadPoolExpression<Integer>() {
                        @Override
                        protected Integer valueForItem(Map.Entry<String, ThreadPools.ThreadPoolExecutorContext> input) {
                            return input.getValue().poolSize();
                        }
                    };
                }
            })
            .put(SysNodesTableInfo.Columns.THREAD_POOLS_QUEUE, new RowCollectExpressionFactory() {
                @Override
                public RowCollectExpression create() {
                    return new NodeStatsThreadPoolExpression<Integer>() {
                        @Override
                        protected Integer valueForItem(Map.Entry<String, ThreadPools.ThreadPoolExecutorContext> input) {
                            return input.getValue().queueSize();
                        }
                    };
                }
            })
            .put(SysNodesTableInfo.Columns.NETWORK, new RowCollectExpressionFactory() {
                @Override
                public RowCollectExpression create() {
                    return new NodeNetworkStatsExpression();
                }
            })
            .put(SysNodesTableInfo.Columns.OS, new RowCollectExpressionFactory() {
                @Override
                public RowCollectExpression create() {
                    return new NodeOsStatsExpression();
                }
            })
            .put(SysNodesTableInfo.Columns.OS_INFO, new RowCollectExpressionFactory() {
                @Override
                public RowCollectExpression create() {
                    return new NodeOsInfoStatsExpression();
                }
            })
            .put(SysNodesTableInfo.Columns.PROCESS, new RowCollectExpressionFactory() {
                @Override
                public RowCollectExpression create() {
                    return new NodeProcessStatsExpression();
                }
            })
            .put(SysNodesTableInfo.Columns.FS, new RowCollectExpressionFactory() {
                @Override
                public RowCollectExpression create() {
                    return new NodeFsStatsExpression();
                }
            })
            .put(SysNodesTableInfo.Columns.FS_TOTAL, new RowCollectExpressionFactory() {
                @Override
                public RowCollectExpression create() {
                    return new NodeFsTotalStatsExpression();
                }
            })
            .put(SysNodesTableInfo.Columns.FS_TOTAL_SIZE, new RowCollectExpressionFactory() {
                @Override
                public RowCollectExpression create() {
                    return new SimpleNodeStatsExpression<Long>() {
                        @Override
                        public Long innerValue() {
                            return this.row.extendedFsStats().total().total();
                        }
                    };
                }
            })
            .put(SysNodesTableInfo.Columns.FS_TOTAL_USED, new RowCollectExpressionFactory() {
                @Override
                public RowCollectExpression create() {
                    return new SimpleNodeStatsExpression<Long>() {
                        @Override
                        public Long innerValue() {
                            return this.row.extendedFsStats().total().used();
                        }
                    };
                }
            })
            .put(SysNodesTableInfo.Columns.FS_TOTAL_AVAILABLE, new RowCollectExpressionFactory() {
                @Override
                public RowCollectExpression create() {
                    return new SimpleNodeStatsExpression<Long>() {
                        @Override
                        public Long innerValue() {
                            return this.row.extendedFsStats().total().available();
                        }
                    };
                }
            })
            .put(SysNodesTableInfo.Columns.FS_TOTAL_READS, new RowCollectExpressionFactory() {
                @Override
                public RowCollectExpression create() {
                    return new SimpleNodeStatsExpression<Long>() {
                        @Override
                        public Long innerValue() {
                            return this.row.extendedFsStats().total().diskReads();
                        }
                    };
                }
            })
            .put(SysNodesTableInfo.Columns.FS_TOTAL_BYTES_READ, new RowCollectExpressionFactory() {
                @Override
                public RowCollectExpression create() {
                    return new SimpleNodeStatsExpression<Long>() {
                        @Override
                        public Long innerValue() {
                            return this.row.extendedFsStats().total().diskReadSizeInBytes();
                        }
                    };
                }
            })
            .put(SysNodesTableInfo.Columns.FS_TOTAL_WRITES, new RowCollectExpressionFactory() {
                @Override
                public RowCollectExpression create() {
                    return new SimpleNodeStatsExpression<Long>() {
                        @Override
                        public Long innerValue() {
                            return this.row.extendedFsStats().total().diskWrites();
                        }
                    };
                }
            })
            .put(SysNodesTableInfo.Columns.FS_TOTAL_BYTES_WRITTEN, new RowCollectExpressionFactory() {
                @Override
                public RowCollectExpression create() {
                    return new SimpleNodeStatsExpression<Long>() {
                        @Override
                        public Long innerValue() {
                            return this.row.extendedFsStats().total().diskWriteSizeInBytes();
                        }
                    };
                }
            })
            .put(SysNodesTableInfo.Columns.FS_DISKS, new RowCollectExpressionFactory() {
                @Override
                public RowCollectExpression create() {
                    return new NodeStatsFsDisksExpression();
                }
            })
            .put(SysNodesTableInfo.Columns.FS_DISKS_DEV, new RowCollectExpressionFactory() {
                @Override
                public RowCollectExpression create() {
                    return new NodeStatsFsArrayExpression<BytesRef>() {
                        @Override
                        protected BytesRef valueForItem(ExtendedFsStats.Info input) {
                            return input.dev();
                        }
                    };
                }
            })
            .put(SysNodesTableInfo.Columns.FS_DISKS_SIZE, new RowCollectExpressionFactory() {
                @Override
                public RowCollectExpression create() {
                    return new NodeStatsFsArrayExpression<Long>() {
                        @Override
                        protected Long valueForItem(ExtendedFsStats.Info input) {
                            return input.total();
                        }
                    };
                }
            })
            .put(SysNodesTableInfo.Columns.FS_DISKS_USED, new RowCollectExpressionFactory() {
                @Override
                public RowCollectExpression create() {
                    return new NodeStatsFsArrayExpression<Long>() {
                        @Override
                        protected Long valueForItem(ExtendedFsStats.Info input) {
                            return input.used();
                        }
                    };
                }
            })
            .put(SysNodesTableInfo.Columns.FS_DISKS_AVAILABLE, new RowCollectExpressionFactory() {
                @Override
                public RowCollectExpression create() {
                    return new NodeStatsFsArrayExpression<Long>() {
                        @Override
                        protected Long valueForItem(ExtendedFsStats.Info input) {
                            return input.available();
                        }
                    };
                }
            })
            .put(SysNodesTableInfo.Columns.FS_DISKS_READS, new RowCollectExpressionFactory() {
                @Override
                public RowCollectExpression create() {
                    return new NodeStatsFsArrayExpression<Long>() {
                        @Override
                        protected Long valueForItem(ExtendedFsStats.Info input) {
                            return input.diskReads();
                        }
                    };
                }
            })
            .put(SysNodesTableInfo.Columns.FS_DISKS_BYTES_READ, new RowCollectExpressionFactory() {
                @Override
                public RowCollectExpression create() {
                    return new NodeStatsFsArrayExpression<Long>() {
                        @Override
                        protected Long valueForItem(ExtendedFsStats.Info input) {
                            return input.diskReadSizeInBytes();
                        }
                    };
                }
            })
            .put(SysNodesTableInfo.Columns.FS_DISKS_WRITES, new RowCollectExpressionFactory() {
                @Override
                public RowCollectExpression create() {
                    return new NodeStatsFsArrayExpression<Long>() {
                        @Override
                        protected Long valueForItem(ExtendedFsStats.Info input) {
                            return input.diskWrites();
                        }
                    };
                }
            })
            .put(SysNodesTableInfo.Columns.FS_DISKS_BYTES_WRITTEN, new RowCollectExpressionFactory() {
                @Override
                public RowCollectExpression create() {
                    return new NodeStatsFsArrayExpression<Long>() {
                        @Override
                        protected Long valueForItem(ExtendedFsStats.Info input) {
                            return input.diskWriteSizeInBytes();
                        }
                    };
                }
            })
            .put(SysNodesTableInfo.Columns.FS_DATA, new RowCollectExpressionFactory() {
                @Override
                public RowCollectExpression create() {
                    return new NodeStatsFsDataExpression();
                }
            })
            .put(SysNodesTableInfo.Columns.FS_DATA_DEV, new RowCollectExpressionFactory() {
                @Override
                public RowCollectExpression create() {
                    return new NodeStatsFsArrayExpression<BytesRef>() {
                        @Override
                        protected BytesRef valueForItem(ExtendedFsStats.Info input) {
                            return input.dev();
                        }
                    };
                }
            })
            .put(SysNodesTableInfo.Columns.FS_DATA_PATH, new RowCollectExpressionFactory() {
                @Override
                public RowCollectExpression create() {
                    return new NodeStatsFsArrayExpression<BytesRef>() {
                        @Override
                        protected BytesRef valueForItem(ExtendedFsStats.Info input) {
                            return input.path();
                        }
                    };
                }
            })
            .build();
    }
}
