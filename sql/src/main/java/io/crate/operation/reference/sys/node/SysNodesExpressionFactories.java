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
import io.crate.metadata.expressions.RowCollectExpressionFactory;
import io.crate.metadata.sys.SysNodesTableInfo;
import io.crate.monitor.ExtendedFsStats;
import io.crate.monitor.ThreadPools;
import io.crate.operation.reference.sys.node.fs.*;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;

import java.util.Map;

public class SysNodesExpressionFactories {

    public SysNodesExpressionFactories() {
    }

    public static Map<ColumnIdent, RowCollectExpressionFactory> getSysNodesTableInfoFactories() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory>builder()
            .put(SysNodesTableInfo.Columns.ID, new RowCollectExpressionFactory() {
                @Override
                public RowCollectExpression create() {
                    return new SimpleDiscoveryNodeExpression<BytesRef>() {
                        @Override
                        public BytesRef innerValue() {
                            return row.id();
                        }
                    };
                }
            })
            .put(SysNodesTableInfo.Columns.NAME, new RowCollectExpressionFactory() {
                @Override
                public RowCollectExpression create() {
                    return new SimpleDiscoveryNodeExpression<BytesRef>() {
                        @Override
                        public BytesRef innerValue() {
                            return row.name();
                        }
                    };
                }
            })
            .put(SysNodesTableInfo.Columns.HOSTNAME, new RowCollectExpressionFactory() {
                @Override
                public RowCollectExpression create() {
                    return new SimpleDiscoveryNodeExpression<BytesRef>() {
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
                    return new SimpleDiscoveryNodeExpression<BytesRef>() {
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
                    return new NodePortExpression();
                }
            })
            .put(SysNodesTableInfo.Columns.LOAD, new RowCollectExpressionFactory() {
                @Override
                public RowCollectExpression create() {
                    return new NodeLoadExpression();
                }
            })
            .put(SysNodesTableInfo.Columns.MEM, new RowCollectExpressionFactory() {
                @Override
                public RowCollectExpression create() {
                    return new NodeMemoryExpression();
                }
            })
            .put(SysNodesTableInfo.Columns.HEAP, new RowCollectExpressionFactory() {
                @Override
                public RowCollectExpression create() {
                    return new NodeHeapExpression();
                }
            })
            .put(SysNodesTableInfo.Columns.VERSION, new RowCollectExpressionFactory() {
                @Override
                public RowCollectExpression create() {
                    return new NodeVersionExpression();
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
                    return new NodeThreadPoolExpression<BytesRef>() {
                        @Override
                        protected BytesRef valueForItem(Map.Entry<String, ThreadPools.ThreadPoolExecutorContext> input) {
                            return BytesRefs.toBytesRef(input.getKey());
                        }
                    };
                }
            })
            .put(SysNodesTableInfo.Columns.THREAD_POOLS_ACTIVE, new RowCollectExpressionFactory() {
                @Override
                public RowCollectExpression create() {
                    return new NodeThreadPoolExpression<Integer>() {
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
                    return new NodeThreadPoolExpression<Long>() {
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
                    return new NodeThreadPoolExpression<Integer>() {
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
                    return new NodeThreadPoolExpression<Long>() {
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
                    return new NodeThreadPoolExpression<Integer>() {
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
                    return new NodeThreadPoolExpression<Integer>() {
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
                    return new NodeNetworkExpression();
                }
            })
            .put(SysNodesTableInfo.Columns.OS, new RowCollectExpressionFactory() {
                @Override
                public RowCollectExpression create() {
                    return new NodeOsExpression();
                }
            })
            .put(SysNodesTableInfo.Columns.OS_INFO, new RowCollectExpressionFactory() {
                @Override
                public RowCollectExpression create() {
                    return new NodeOsInfoExpression();
                }
            })
            .put(SysNodesTableInfo.Columns.PROCESS, new RowCollectExpressionFactory() {
                @Override
                public RowCollectExpression create() {
                    return new NodeProcessExpression();
                }
            })
            .put(SysNodesTableInfo.Columns.FS, new RowCollectExpressionFactory() {
                @Override
                public RowCollectExpression create() {
                    return new NodeFsExpression();
                }
            })
            .put(SysNodesTableInfo.Columns.FS_TOTAL, new RowCollectExpressionFactory() {
                @Override
                public RowCollectExpression create() {
                    return new NodeFsTotalExpression();
                }
            })
            .put(SysNodesTableInfo.Columns.FS_TOTAL_SIZE, new RowCollectExpressionFactory() {
                @Override
                public RowCollectExpression create() {
                    return new NodeFsTotalExpression.Item() {
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
                    return new NodeFsTotalExpression.Item() {
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
                    return new NodeFsTotalExpression.Item() {
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
                    return new NodeFsTotalExpression.Item() {
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
                    return new NodeFsTotalExpression.Item() {
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
                    return new NodeFsTotalExpression.Item() {
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
                    return new NodeFsTotalExpression.Item() {
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
                    return new NodeFsDisksExpression();
                }
            })
            .put(SysNodesTableInfo.Columns.FS_DISKS_DEV, new RowCollectExpressionFactory() {
                @Override
                public RowCollectExpression create() {
                    return new NodeFsDisksExpression.Item<BytesRef>() {
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
                    return new NodeFsDisksExpression.Item<Long>() {
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
                    return new NodeFsDisksExpression.Item<Long>() {
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
                    return new NodeFsDisksExpression.Item<Long>() {
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
                    return new NodeFsDisksExpression.Item<Long>() {
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
                    return new NodeFsDisksExpression.Item<Long>() {
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
                    return new NodeFsDisksExpression.Item<Long>() {
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
                    return new NodeFsDisksExpression.Item<Long>() {
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
                    return new NodeFsDataExpression();
                }
            })
            .put(SysNodesTableInfo.Columns.FS_DATA_DEV, new RowCollectExpressionFactory() {
                @Override
                public RowCollectExpression create() {
                    return new NodeFsDataExpression.Item<BytesRef>() {
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
                    return new NodeFsDataExpression.Item<BytesRef>() {
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
