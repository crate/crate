package io.crate.operation.reference.sys.shard.unassigned;

import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.ReferenceImplementation;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.SimpleObjectExpression;
import io.crate.metadata.shard.unassigned.UnassignedShardCollectorExpression;
import io.crate.metadata.sys.SysClusterTableInfo;
import io.crate.metadata.sys.SysNodesTableInfo;
import io.crate.operation.reference.DocLevelReferenceResolver;
import io.crate.operation.reference.sys.shard.*;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.inject.Inject;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

public class UnassignedShardsReferenceResolver implements DocLevelReferenceResolver<UnassignedShardCollectorExpression<?>> {

    private static final Map<ReferenceInfo, UnassignedShardCollectorExpression<?>> IMPLEMENTATIONS = new HashMap<>();
    private final Map<ReferenceIdent, ReferenceImplementation> referenceImplementations;

    private static void register(UnassignedShardCollectorExpression<?> collectorExpression) {
        IMPLEMENTATIONS.put(collectorExpression.info(), collectorExpression);
    }

    static {
        register(new UnassignedShardCollectorExpression<BytesRef>(ShardSchemaNameExpression.NAME) {
            @Override
            public BytesRef value() {
                return new BytesRef(this.row.schemaName());
            }
        });
        register(new UnassignedShardCollectorExpression<BytesRef>(ShardTableNameExpression.NAME) {
            @Override
            public BytesRef value() {
                return new BytesRef(this.row.tableName());
            }
        });
        register(new UnassignedShardCollectorExpression<BytesRef>(ShardPartitionIdentExpression.NAME) {
            @Override
            public BytesRef value() {
                return new BytesRef(this.row.partitionIdent());
            }
        });
        register(new UnassignedShardCollectorExpression<Integer>(ShardIdExpression.NAME) {
            @Override
            public Integer value() {
                return this.row.id();
            }
        });
        register(new UnassignedShardCollectorExpression<Long>(ShardNumDocsExpression.NAME) {
            @Override
            public Long value() {
                return 0L;
            }
        });
        register(new UnassignedShardCollectorExpression<Boolean>(ShardPrimaryExpression.NAME) {
            @Override
            public Boolean value() {
                return row.primary();
            }
        });
        register(new UnassignedShardCollectorExpression<BytesRef>(ShardRelocatingNodeExpression.NAME) {
            @Override
            public BytesRef value() {
                return null;
            }
        });
        register(new UnassignedShardCollectorExpression<Long>(ShardSizeExpression.NAME) {
            @Override
            public Long value() {
                return 0L;
            }
        });
        register(new UnassignedShardCollectorExpression<BytesRef>(ShardStateExpression.NAME) {
            @Override
            public BytesRef value() {
                return row.state();
            }
        });
        register(new UnassignedShardCollectorExpression<Boolean>(ShardPartitionOrphanedExpression.NAME) {
            @Override
            public Boolean value() {
                return this.row.orphanedPartition();
            }
        });
        register(new UnassignedShardCollectorExpression(SysNodesTableInfo.SYS_COL_NAME) {
            @Override
            public Object value() {
                return null;
            }
        });

        for (ReferenceInfo referenceInfo : SysNodesTableInfo.INFOS.values()) {
            register(nullExpression(referenceInfo));
        }
    }

    @Inject
    public UnassignedShardsReferenceResolver(Map<ReferenceIdent, ReferenceImplementation> referenceImplementations) {
        this.referenceImplementations = referenceImplementations;
    }

    @Nullable
    @Override
    public UnassignedShardCollectorExpression<?> getImplementation(ReferenceInfo info) {
        UnassignedShardCollectorExpression<?> expression = IMPLEMENTATIONS.get(info);
        if (expression == null) {
            if (info.ident().tableIdent().equals(SysClusterTableInfo.IDENT)) {
                final ReferenceImplementation referenceImplementation = referenceImplementations.get(info.ident());
                if (referenceImplementation == null) {
                    return null;
                }
                assert referenceImplementation instanceof SimpleObjectExpression;
                return new UnassignedShardCollectorExpression<Object>(info) {
                    @Override
                    public Object value() {
                        return (referenceImplementation).value();
                    }
                };
            } else if (info.ident().columnIdent().name().equals(SysNodesTableInfo.SYS_COL_NAME)) {
                return nullExpression(info);
            }
        }
        return expression;
    }

    private static <T> NullUnassignedShardCollectorExpression<T> nullExpression(ReferenceInfo info) {
        return new NullUnassignedShardCollectorExpression<>(info);
    }

    static class NullUnassignedShardCollectorExpression<T> extends UnassignedShardCollectorExpression<T> {

        protected NullUnassignedShardCollectorExpression(ReferenceInfo info) {
            super(info);
        }

        @Override
        public T value() {
            return null;
        }
    }
}
