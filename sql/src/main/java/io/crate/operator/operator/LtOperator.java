package io.crate.operator.operator;

import io.crate.metadata.FunctionInfo;
import org.cratedb.DataType;

public class LtOperator extends CmpOperator {

    public static final String NAME = "op_<";

    public static void register(OperatorModule module) {
        for (DataType type : DataType.PRIMITIVE_TYPES) {
            module.registerOperatorFunction(new LtOperator(generateInfo(NAME, type)));
        }
    }

    LtOperator(FunctionInfo info) {
        super(info);
    }

    @Override
    protected boolean compare(int comparisonResult) {
        return comparisonResult == -1;
    }
}
