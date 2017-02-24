package io.crate.operation.reference.sys.shard.unassigned;

import com.google.common.collect.ImmutableMap;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.ReferenceImplementation;
import io.crate.metadata.RowContextCollectorExpression;
import io.crate.metadata.expressions.RowCollectExpressionFactory;
import io.crate.metadata.shard.unassigned.UnassignedShard;
import io.crate.metadata.sys.SysNodesTableInfo;
import io.crate.metadata.sys.SysShardsTableInfo;
import org.apache.lucene.util.BytesRef;

import java.util.Map;

public class UnassignedShardsExpressionFactories {

    private UnassignedShardsExpressionFactories() {
    }

    public static Map<ColumnIdent, RowCollectExpressionFactory> getSysShardsTableInfoFactories() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory>builder()
            .put(SysShardsTableInfo.Columns.SCHEMA_NAME, new RowCollectExpressionFactory() {

                @Override
                public RowContextCollectorExpression create() {
                    return new RowContextCollectorExpression<UnassignedShard, BytesRef>() {
                        @Override
                        public BytesRef value() {
                            return new BytesRef(this.row.schemaName());
                        }
                    };
                }
            })
            .put(SysShardsTableInfo.Columns.TABLE_NAME, new RowCollectExpressionFactory() {
                @Override
                public RowContextCollectorExpression create() {
                    return new RowContextCollectorExpression<UnassignedShard, BytesRef>() {
                        @Override
                        public BytesRef value() {
                            return new BytesRef(this.row.tableName());
                        }
                    };
                }
            })
            .put(SysShardsTableInfo.Columns.PARTITION_IDENT, new RowCollectExpressionFactory() {
                @Override
                public RowContextCollectorExpression create() {
                    return new RowContextCollectorExpression<UnassignedShard, BytesRef>() {
                        @Override
                        public BytesRef value() {
                            return new BytesRef(this.row.partitionIdent());
                        }
                    };
                }
            })
            .put(SysShardsTableInfo.Columns.ID, new RowCollectExpressionFactory() {
                @Override
                public RowContextCollectorExpression create() {
                    return new RowContextCollectorExpression<UnassignedShard, Integer>() {
                        @Override
                        public Integer value() {
                            return this.row.id();
                        }
                    };
                }
            })
            .put(SysShardsTableInfo.Columns.NUM_DOCS, () -> new RowContextCollectorExpression<UnassignedShard, Long>() {
                @Override
                public Long value() {
                    return 0L;
                }
            })
            .put(SysShardsTableInfo.Columns.PRIMARY, () -> new RowContextCollectorExpression<UnassignedShard, Boolean>() {
                @Override
                public Boolean value() {
                    return row.primary();
                }
            })
            .put(SysShardsTableInfo.Columns.RELOCATING_NODE, () -> new RowContextCollectorExpression<UnassignedShard, BytesRef>() {
                @Override
                public BytesRef value() {
                    return null;
                }
            })
            .put(SysShardsTableInfo.Columns.SIZE, () -> new RowContextCollectorExpression<UnassignedShard, Long>() {
                @Override
                public Long value() {
                    return 0L;
                }
            })
            .put(SysShardsTableInfo.Columns.STATE, () -> new RowContextCollectorExpression<UnassignedShard, BytesRef>() {
                @Override
                public BytesRef value() {
                    return row.state();
                }
            })
            .put(SysShardsTableInfo.Columns.ROUTING_STATE, () -> new RowContextCollectorExpression<UnassignedShard, BytesRef>() {
                @Override
                public BytesRef value() {
                    return row.state();
                }
            })
            .put(SysShardsTableInfo.Columns.ORPHAN_PARTITION, new RowCollectExpressionFactory() {
                @Override
                public RowContextCollectorExpression create() {
                    return new RowContextCollectorExpression<UnassignedShard, Boolean>() {
                        @Override
                        public Boolean value() {
                            return this.row.orphanedPartition();
                        }
                    };
                }
            })
            .put(SysShardsTableInfo.Columns.RECOVERY, new RowCollectExpressionFactory() {
                @Override
                public RowContextCollectorExpression create() {
                    return new RowContextCollectorExpression() {
                        @Override
                        public Object value() {
                            return null;
                        }

                        @Override
                        public ReferenceImplementation getChildImplementation(String name) {
                            return this;
                        }
                    };
                }
            })
            .put(SysNodesTableInfo.SYS_COL_IDENT, new RowCollectExpressionFactory() {
                @Override
                public RowContextCollectorExpression create() {
                    return new RowContextCollectorExpression() {
                        @Override
                        public Object value() {
                            return null;
                        }

                        @Override
                        public ReferenceImplementation getChildImplementation(String name) {
                            return this;
                        }
                    };
                }
            })
            .put(SysShardsTableInfo.Columns.PATH, () -> new RowContextCollectorExpression() {
                @Override
                public Object value() {
                    return null;
                }
            })
            .put(SysShardsTableInfo.Columns.BLOB_PATH, () -> new RowContextCollectorExpression() {
                @Override
                public Object value() {
                    return null;
                }
            })
            .put(SysShardsTableInfo.Columns.MIN_LUCENE_VERSION, () -> new RowContextCollectorExpression<UnassignedShard, String>() {
                @Override
                public String value() {
                    return null;
                }
            })
            .build();
    }
}
