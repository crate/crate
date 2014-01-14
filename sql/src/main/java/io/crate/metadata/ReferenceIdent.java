package io.crate.metadata;

import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Ordering;

import java.util.List;

public class ReferenceIdent implements Comparable<ReferenceIdent> {

    private String schema;
    private String table;
    private String column;
    private List<String> path;

    public ReferenceIdent(String schema, String table, String column, List<String> path) {
        this(schema, table, column);
        this.path = path;
    }

    public ReferenceIdent(String schema, String table, String column) {
        this.schema = schema;
        this.table = table;
        this.column = column;
    }



    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        ReferenceIdent o = (ReferenceIdent) obj;
        return Objects.equal(schema, o.schema) &&
                Objects.equal(table, o.table) &&
                Objects.equal(column, o.column) &&
                Objects.equal(path, o.path);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(schema, table, column, path);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("schema", schema)
                .add("table", table)
                .add("column", column)
                .add("path", path)
                .toString();
    }

    @Override
    public int compareTo(ReferenceIdent o) {
        return ComparisonChain.start()
                .compare(schema, o.schema)
                .compare(table, o.table)
                .compare(column, o.column)
                .compare(path, o.path, Ordering.<String>natural().lexicographical())
                .result();
    }


}
