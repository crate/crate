package io.crate.metadata.shard.unassigned;


import io.crate.metadata.*;
import io.crate.metadata.sys.SysShardsTableInfo;

public abstract class UnassignedShardCollectorExpression<T> extends RowContextCollectorExpression<UnassignedShard, T> {

    private final ReferenceInfo info;

    protected UnassignedShardCollectorExpression(String name) {
        this.info = SysShardsTableInfo.INFOS.get(new ColumnIdent(name));
    }

    protected UnassignedShardCollectorExpression(ReferenceInfo info) {
        this.info = info;
    }

    @Deprecated
    public ReferenceInfo info() {
        return info;
    }
}
