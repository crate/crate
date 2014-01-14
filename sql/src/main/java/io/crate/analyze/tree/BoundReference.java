package io.crate.analyze.tree;

import io.crate.metadata.ReferenceInfo;
import io.crate.sql.tree.Expression;

public class BoundReference extends Expression {

    private final ReferenceInfo info;

    public BoundReference(ReferenceInfo info) {
        this.info = info;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BoundReference that = (BoundReference) o;

        if (!info.equals(that.info)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return info.hashCode();
    }
}
