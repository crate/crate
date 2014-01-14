package io.crate.metadata;

import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Ordering;
import org.cratedb.DataType;

import java.util.List;

public class FunctionIdent implements Comparable<FunctionIdent> {

    private final String name;
    private final List<DataType> argumentTypes;

    public FunctionIdent(String name, List<DataType> argumentTypes) {
        this.name = name;
        this.argumentTypes = argumentTypes;
    }

    public List<DataType> argumentTypes() {
        return argumentTypes;
    }

    public String name() {
        return name;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }

        FunctionIdent o = (FunctionIdent) obj;
        return Objects.equal(name, o.name) &&
                Objects.equal(argumentTypes, o.argumentTypes);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(name, argumentTypes);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("name", name)
                .add("argumentTypes", argumentTypes)
                .toString();
    }

    @Override
    public int compareTo(FunctionIdent o) {
        return ComparisonChain.start()
                .compare(name, o.name)
                .compare(argumentTypes, o.argumentTypes, Ordering.<DataType>natural().lexicographical())
                .result();
    }


}
