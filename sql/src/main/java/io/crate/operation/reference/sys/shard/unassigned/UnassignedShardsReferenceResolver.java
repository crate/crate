package io.crate.operation.reference.sys.shard.unassigned;

import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.ReferenceImplementation;
import io.crate.metadata.ReferenceInfo;
import com.google.common.collect.ImmutableMap;
import io.crate.metadata.*;
import io.crate.metadata.expressions.RowCollectExpressionFactory;
import io.crate.metadata.shard.unassigned.UnassignedShard;
import io.crate.metadata.sys.SysNodesTableInfo;
import io.crate.metadata.sys.SysShardsTableInfo;
import io.crate.operation.reference.DocLevelReferenceResolver;
import io.crate.operation.reference.sys.job.RowContextDocLevelReferenceResolver;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.inject.Inject;

import javax.annotation.Nullable;
import java.util.Map;

public class UnassignedShardsReferenceResolver implements DocLevelReferenceResolver<RowCollectExpression<?, ?>> {

    private final Map<TableIdent, Map<ColumnIdent, RowCollectExpressionFactory>> factoryMap;

    @Inject
    public UnassignedShardsReferenceResolver() {
        ImmutableMap.Builder<TableIdent, Map<ColumnIdent, RowCollectExpressionFactory>> builder = ImmutableMap.builder();
        builder.put(SysShardsTableInfo.IDENT, getSysShardsTableInfoFactories());
        factoryMap = builder.build();
    }

    private Map<ColumnIdent, RowCollectExpressionFactory> getSysShardsTableInfoFactories() {
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
                .put(SysShardsTableInfo.Columns.NUM_DOCS, new RowCollectExpressionFactory() {
                    @Override
                    public RowContextCollectorExpression create() {
                        return new RowContextCollectorExpression<UnassignedShard, Long>() {
                            @Override
                            public Long value() {
                                return 0L;
                            }
                        };
                    }
                })
                .put(SysShardsTableInfo.Columns.PRIMARY, new RowCollectExpressionFactory() {
                    @Override
                    public RowContextCollectorExpression create() {
                        return new RowContextCollectorExpression<UnassignedShard, Boolean>() {
                            @Override
                            public Boolean value() {
                                return row.primary();
                            }
                        };
                    }
                })
                .put(SysShardsTableInfo.Columns.RELOCATING_NODE, new RowCollectExpressionFactory() {
                    @Override
                    public RowContextCollectorExpression create() {
                        return new RowContextCollectorExpression<UnassignedShard, BytesRef>() {
                            @Override
                            public BytesRef value() {
                                return null;
                            }
                        };
                    }
                })
                .put(SysShardsTableInfo.Columns.SIZE, new RowCollectExpressionFactory() {
                    @Override
                    public RowContextCollectorExpression create() {
                        return new RowContextCollectorExpression<UnassignedShard, Long>() {
                            @Override
                            public Long value() {
                                return 0L;
                            }
                        };
                    }
                })
                .put(SysShardsTableInfo.Columns.STATE, new RowCollectExpressionFactory() {
                    @Override
                    public RowContextCollectorExpression create() {
                        return new RowContextCollectorExpression<UnassignedShard, BytesRef>() {
                            @Override
                            public BytesRef value() {
                                return row.state();
                            }
                        };
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
                .put(SysNodesTableInfo.SYS_COL_IDENT, new RowCollectExpressionFactory() {
                    @Override
                    public RowContextCollectorExpression create() {
                        return new RowContextCollectorExpression() {
                            @Override
                            public Object value() {
                                return null;
                            }
                        };
                    }
                })
                .build();
    }


    @Nullable
    @Override
    public RowCollectExpression<?, ?> getImplementation(ReferenceInfo info) {
        RowCollectExpression expression =  RowContextDocLevelReferenceResolver.rowCollectExpressionFromFactoryMap(factoryMap, info);
        if (expression == null && info.ident().columnIdent().name().equals(SysNodesTableInfo.SYS_COL_NAME)) {
            return new RowContextCollectorExpression<UnassignedShard, Object>() {
                @Override
                public Object value() {
                    return null;
                }
            };
        }
        return expression;
    }
}
