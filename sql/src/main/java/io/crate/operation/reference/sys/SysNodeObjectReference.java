package io.crate.operation.reference.sys;

import io.crate.metadata.ColumnIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.sys.SysNodesTableInfo;
import org.elasticsearch.common.Preconditions;

public class SysNodeObjectReference extends SysObjectReference {

    private final ReferenceInfo info;

    protected SysNodeObjectReference(String name) {
        this(new ColumnIdent(name));
    }

    protected SysNodeObjectReference(ColumnIdent ident) {
        info = SysNodesTableInfo.INFOS.get(ident);
        Preconditions.checkNotNull(info, "info");
    }

    @Override
    public ReferenceInfo info() {
        return info;
    }
}
