package io.crate.operation.operator;

import io.crate.metadata.FunctionInfo;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

public class LtOperator extends CmpOperator {

    public static final String NAME = "op_<";

    LtOperator(FunctionInfo info) {
        super(info);
    }

    public static void register(OperatorModule module) {
        for (DataType type : DataTypes.PRIMITIVE_TYPES) {
            module.registerOperatorFunction(new LtOperator(generateInfo(NAME, type)));
        }
    }

    @Override
    protected boolean compare(int comparisonResult) {
        return comparisonResult < 0;
    }
}
