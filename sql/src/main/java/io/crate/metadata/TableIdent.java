package io.crate.metadata;

import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Ordering;

import java.util.List;

public class TableIdent implements Comparable<TableIdent> {

    private String schema;
    private String name;

    public TableIdent(String schema, String name) {
        this.schema = schema;
        this.name = name;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        TableIdent o = (TableIdent) obj;
        return Objects.equal(schema, o.schema) &&
                Objects.equal(name, o.name);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(schema, name);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("schema", schema)
                .add("name", name)
                .toString();
    }

    @Override
    public int compareTo(TableIdent o) {
        return ComparisonChain.start()
                .compare(schema, o.schema)
                .compare(name, o.name)
                .result();
    }


}
