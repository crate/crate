package io.crate.operation.operator;

import com.google.common.collect.ImmutableList;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.operation.Input;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

public abstract class Operator<I> extends Scalar<Boolean, I> implements FunctionImplementation<Function> {

    public static final io.crate.types.DataType RETURN_TYPE = DataTypes.BOOLEAN;

    protected static FunctionInfo generateInfo(String name, DataType type) {
        return new FunctionInfo(new FunctionIdent(name, ImmutableList.of(type, type)), RETURN_TYPE);
    }

    protected boolean containsNull(Symbol left, Symbol right) {
        if (left.symbolType().isValueSymbol() && ((Input<?>)left).value() == null) {
            return true;
        }
        if (right.symbolType().isValueSymbol() && ((Input<?>)right).value() == null) {
            return true;
        }
        return false;
    }
}
