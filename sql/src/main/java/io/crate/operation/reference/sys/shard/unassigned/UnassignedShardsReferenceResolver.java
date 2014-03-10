package io.crate.operation.reference.sys.shard.unassigned;

import com.google.common.collect.ImmutableList;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.shard.unassigned.UnassignedShardCollectorExpression;
import io.crate.operation.reference.DocLevelReferenceResolver;
import io.crate.operation.reference.sys.shard.*;
import org.apache.lucene.util.BytesRef;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

public class UnassignedShardsReferenceResolver implements DocLevelReferenceResolver<UnassignedShardCollectorExpression<?>> {

    private static final ImmutableList<UnassignedShardCollectorExpression<?>> IMPLEMENTATIONS =
        ImmutableList.<UnassignedShardCollectorExpression<?>>builder()
        .add(new UnassignedShardCollectorExpression<BytesRef>(ShardSchemaNameExpression.NAME) {
            @Override
            public BytesRef value() {
                return new BytesRef(this.row.schemaName());
            }
        })
        .add(new UnassignedShardCollectorExpression<BytesRef>(ShardTableNameExpression.NAME) {
            @Override
            public BytesRef value() {
                return new BytesRef(this.row.tableName());
            }
        })
        .add(new UnassignedShardCollectorExpression<Integer>(ShardIdExpression.NAME) {
            @Override
            public Integer value() {
                return this.row.id();
            }
        })
        .add(new UnassignedShardCollectorExpression<Long>(ShardNumDocsExpression.NAME) {
            @Override
            public Long value() {
                return 0L;
            }
        })
        .add(new UnassignedShardCollectorExpression<Boolean>(ShardPrimaryExpression.NAME) {
            @Override
            public Boolean value() {
                return false;
            }
        })
        .add(new UnassignedShardCollectorExpression<BytesRef>(ShardRelocatingNodeExpression.NAME) {
            @Override
            public BytesRef value() {
                return null;
            }
        })
        .add(new UnassignedShardCollectorExpression<Long>(ShardSizeExpression.NAME) {
            @Override
            public Long value() {
                return 0L;
            }
        })
        .add(new UnassignedShardCollectorExpression<BytesRef>(ShardStateExpression.NAME) {
            @Override
            public BytesRef value() {
                return new BytesRef("UNASSIGNED");
            }
        }).build();

    private final Map<ReferenceInfo, UnassignedShardCollectorExpression<?>> implementations;

    public UnassignedShardsReferenceResolver() {
        implementations = new HashMap<>(IMPLEMENTATIONS.size());
        for (UnassignedShardCollectorExpression<?> implementation : IMPLEMENTATIONS) {
            implementations.put(implementation.info(), implementation);
        }
    }

    @Nullable
    @Override
    public UnassignedShardCollectorExpression<?> getImplementation(ReferenceInfo info) {
        return implementations.get(info);
    }
}
