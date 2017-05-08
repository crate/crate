package io.crate.operation.scalar.arithmetic;

import io.crate.data.Input;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.types.DataType;

import java.util.Arrays;
import java.util.Set;
import java.util.function.BinaryOperator;

public final class BinaryScalar<T> extends Scalar<T, T> {

    private final BinaryOperator<T> func;
    private final FunctionInfo info;
    private final DataType<T> type;

    public BinaryScalar(BinaryOperator<T> func, String name, DataType<T> type, Set<FunctionInfo.Feature> feature) {
        this.func = func;
        this.info = new FunctionInfo(new FunctionIdent(name, Arrays.asList(type, type)), type, FunctionInfo.Type.SCALAR, feature);
        this.type = type;
    }

    @Override
    public FunctionInfo info() {
        return info;
    }


    @Override
    public T evaluate(Input<T>[] args) {
        T arg0Value = type.value(args[0].value());
        T arg1Value = type.value(args[1].value());

        if (arg0Value == null) {
            return null;
        }
        if (arg1Value == null) {
            return null;
        }
        return func.apply(arg0Value, arg1Value);
    }
}
