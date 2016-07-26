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
import io.crate.operation.reference.sys.node.fs.NodeFsExpression;
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
                            return BytesRefs.toBytesRef(row.id);
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
                            return BytesRefs.toBytesRef(row.name);
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
                            return BytesRefs.toBytesRef(row.hostname);
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
                            return BytesRefs.toBytesRef(row.restUrl);
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
            .build();
    }
}
