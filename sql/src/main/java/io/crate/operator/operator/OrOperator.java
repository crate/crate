package io.crate.operator.operator;

import com.google.common.collect.ImmutableList;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionInfo;
import org.cratedb.DataType;

public class OrOperator implements FunctionImplementation {

    public static final String NAME = "op_or";
    private final FunctionInfo info;

    @Override
    public FunctionInfo info() {
        return info;
    }

    public static void register(OperatorModule module) {
        module.registerOperatorFunction(
                new OrOperator(
                        new FunctionInfo(
                            new FunctionIdent(NAME, ImmutableList.of(DataType.BOOLEAN, DataType.BOOLEAN)),
                            DataType.BOOLEAN
                        )
                )
        );
    }

    OrOperator(FunctionInfo info) {
        this.info = info;
    }
}
