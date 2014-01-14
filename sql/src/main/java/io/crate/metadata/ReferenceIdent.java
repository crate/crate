package io.crate.metadata;

import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Ordering;

import java.util.List;

public class ReferenceIdent implements Comparable<ReferenceIdent> {

    private TableIdent tableIdent;
    private String column;
    private List<String> path;

    public ReferenceIdent(TableIdent tableIdent, String column) {
        this.tableIdent = tableIdent;
        this.column = column;
    }

    public ReferenceIdent(TableIdent tableIdent, String column, List<String> path) {
        this(tableIdent, column);
        this.path = path;
    }

    public TableIdent tableIdent() {
        return tableIdent;
    }

    public String column() {
        return column;
    }

    public List<String> path() {
        return path;
    }

    public boolean isColumn() {
        return path == null || path.size()==0;
    }

    public ReferenceIdent columnIdent(){
        if (isColumn()){
            return this;
        }
        return new ReferenceIdent(tableIdent, column);
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
        return Objects.equal(column, o.column) &&
                Objects.equal(tableIdent, o.tableIdent) &&
                Objects.equal(path, o.path);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(tableIdent, column, path);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("table", tableIdent)
                .add("column", column)
                .add("path", path)
                .toString();
    }

    @Override
    public int compareTo(ReferenceIdent o) {
        return ComparisonChain.start()
                .compare(tableIdent, o.tableIdent)
                .compare(column, o.column)
                .compare(path, o.path, Ordering.<String>natural().lexicographical())
                .result();
    }


}
