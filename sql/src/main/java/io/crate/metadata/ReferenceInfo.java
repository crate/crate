package io.crate.metadata;


import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;
import io.crate.planner.RowGranularity;
import org.cratedb.DataType;

public class ReferenceInfo
        implements Comparable<ReferenceInfo> {

    private final ReferenceIdent ident;
    private final DataType type;
    private final RowGranularity granularity;

    public ReferenceInfo(ReferenceIdent ident, RowGranularity granularity, DataType type) {
        this.ident = ident;
        this.type = type;
        this.granularity = granularity;
    }

    public ReferenceIdent ident() {
        return ident;
    }

    public DataType type() {
        return type;
    }

    public RowGranularity granularity() {
        return granularity;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }

        ReferenceInfo o = (ReferenceInfo) obj;
        return Objects.equal(granularity, o.granularity) &&
                Objects.equal(ident, o.ident) &&
                Objects.equal(type, o.type);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(granularity, ident, type);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("granularity", type)
                .add("ident", ident)
                .add("type", type)
                .toString();
    }

    @Override
    public int compareTo(ReferenceInfo o) {
        return ComparisonChain.start()
                .compare(granularity, o.granularity)
                .compare(ident, o.ident)
                .compare(type, o.type)
                .result();
    }

}
