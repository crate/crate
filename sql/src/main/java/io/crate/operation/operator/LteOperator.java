package io.crate.operation.operator;

import io.crate.metadata.FunctionInfo;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

public class LteOperator extends CmpOperator {

    public static final String NAME = "op_<=";

    public static void register(OperatorModule module) {
        for (DataType type : DataTypes.PRIMITIVE_TYPES) {
            module.registerOperatorFunction(new LteOperator(generateInfo(NAME, type)));
        }
    }

    LteOperator(FunctionInfo info) {
        super(info);
    }

    @Override
    protected boolean compare(int comparisonResult) {
        return comparisonResult <= 0;
    }
}
