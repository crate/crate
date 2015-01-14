package io.crate.operation.reference.sys;

import io.crate.metadata.ColumnIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.sys.SysNodesTableInfo;
import io.crate.operation.reference.sys.node.SysNodeExpression;
import org.elasticsearch.common.Preconditions;

public abstract class SysNodeObjectReference extends SysObjectReference {

    private final ReferenceInfo info;

    protected SysNodeObjectReference(String name) {
        this(new ColumnIdent(name));
    }

    protected SysNodeObjectReference(ColumnIdent ident) {
        info = SysNodesTableInfo.INFOS.get(ident);
        Preconditions.checkNotNull(info, "info");
    }

    @Deprecated
    public ReferenceInfo info() {
        return info;
    }

    protected abstract class ChildExpression<T> extends SysNodeExpression<T> {

        protected ChildExpression(String name) {
            super(ColumnIdent.getChild(SysNodeObjectReference.this.info.ident().columnIdent(), name));
        }
    }
}
