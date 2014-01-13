package io.crate.metadata;


// PRESTOBORROW

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Ordering;
import io.crate.sql.tree.QualifiedName;
import org.cratedb.DataType;

import java.util.List;

public class FunctionInfo
        implements Comparable<FunctionInfo> {
    //private final int id;
    private final FunctionIdent ident;
    private final String description;
    private final DataType returnType;
    private final boolean isAggregate;
    //private final Type intermediateType;
    //private final AggregationFunction aggregationFunction;

    //private final MethodHandle scalarFunction;
    private final boolean deterministic;
    //private final FunctionBinder functionBinder;

    // private final boolean isWindow;
    //private final Supplier<WindowFunction> windowFunction;

    public FunctionInfo(QualifiedName name, String description, DataType returnType, List<DataType> argumentTypes) {
        this.ident = new FunctionIdent(name, argumentTypes);
        this.description = description;
        this.returnType = returnType;
        this.deterministic = true;
        this.isAggregate = false;
    }

    public FunctionInfo(QualifiedName name, String description, DataType returnType, List<DataType> argumentTypes, boolean isAggregate) {
        this.ident = new FunctionIdent(name, argumentTypes);
        this.description = description;
        this.returnType = returnType;
        this.isAggregate = isAggregate;
        this.deterministic = true;
    }

    public FunctionIdent ident() {
        return ident;
    }

    public String description() {
        return description;
    }

    public boolean isAggregate() {
        return isAggregate;
    }


    public DataType returnType() {
        return returnType;
    }

    public boolean isDeterministic() {
        return deterministic;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }

        FunctionInfo o = (FunctionInfo) obj;
        return Objects.equal(isAggregate, o.isAggregate) &&
                Objects.equal(ident, o.ident) &&
                Objects.equal(returnType, o.returnType);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(isAggregate, ident, returnType);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("isAggregate", isAggregate)
                .add("ident", ident)
                .add("returnType", returnType)
                .toString();
    }

    @Override
    public int compareTo(FunctionInfo o) {
        return ComparisonChain.start()
                .compareTrueFirst(isAggregate, o.isAggregate)
                .compare(ident, o.ident)
                .compare(returnType, o.returnType)
                .result();
    }

    public static Function<FunctionInfo, QualifiedName> nameGetter() {
        return new Function<FunctionInfo, QualifiedName>() {
            @Override
            public QualifiedName apply(FunctionInfo input) {
                return input.ident().name();
            }
        };
    }

    public static Predicate<FunctionInfo> isAggregationPredicate() {
        return new Predicate<FunctionInfo>() {
            @Override
            public boolean apply(FunctionInfo functionInfo) {
                return functionInfo.isAggregate();
            }
        };
    }
}
