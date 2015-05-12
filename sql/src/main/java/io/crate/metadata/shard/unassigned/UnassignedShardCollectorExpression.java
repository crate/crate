package io.crate.metadata.shard.unassigned;


import io.crate.metadata.ColumnIdent;
import io.crate.metadata.ReferenceImplementation;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.sys.SysShardsTableInfo;
import io.crate.operation.Input;

public abstract class UnassignedShardCollectorExpression<T> implements ReferenceImplementation<T> {

    private final ReferenceInfo info;
    protected UnassignedShard row;

    protected UnassignedShardCollectorExpression(String name) {
        this.info = SysShardsTableInfo.INFOS.get(new ColumnIdent(name));
    }

    protected UnassignedShardCollectorExpression(ReferenceInfo info) {
        this.info = info;
    }

    @Override
    public ReferenceImplementation getChildImplementation(String name) {
        return null;
    }

    @Deprecated
    public ReferenceInfo info() {
        return info;
    }

    public void setNextRow(UnassignedShard row) {
        this.row = row;
    }
}
