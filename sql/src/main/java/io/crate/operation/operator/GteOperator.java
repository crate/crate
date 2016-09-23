package io.crate.operation.operator;

import io.crate.metadata.FunctionInfo;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

public class GteOperator extends CmpOperator {

    public static final String NAME = "op_>=";

    GteOperator(FunctionInfo info) {
        super(info);
    }

    public static void register(OperatorModule module) {
        for (DataType type : DataTypes.PRIMITIVE_TYPES) {
            module.registerOperatorFunction(new GteOperator(generateInfo(NAME, type)));
        }
    }

    @Override
    protected boolean compare(int comparisonResult) {
        return comparisonResult >= 0;
    }
}
