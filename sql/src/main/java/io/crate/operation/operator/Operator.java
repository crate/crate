package io.crate.operation.operator;

import com.google.common.collect.ImmutableList;
import io.crate.analyze.symbol.Function;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

public abstract class Operator<I> extends Scalar<Boolean, I> implements FunctionImplementation<Function> {

    public static final io.crate.types.DataType RETURN_TYPE = DataTypes.BOOLEAN;

    protected static FunctionInfo generateInfo(String name, DataType type) {
        return new FunctionInfo(new FunctionIdent(name, ImmutableList.of(type, type)), RETURN_TYPE);
    }
}
