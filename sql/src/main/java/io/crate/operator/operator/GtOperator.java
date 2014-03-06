package io.crate.operator.operator;

import io.crate.metadata.FunctionInfo;
import io.crate.DataType;

public class GtOperator extends CmpOperator {

    public static final String NAME = "op_>";

    public static void register(OperatorModule module) {
        for (DataType type : DataType.PRIMITIVE_TYPES) {
            module.registerOperatorFunction(new GtOperator(generateInfo(NAME, type)));
        }
    }

    GtOperator(FunctionInfo info) {
        super(info);
    }

    @Override
    protected boolean compare(int comparisonResult) {
        return comparisonResult == 1;
    }
}
