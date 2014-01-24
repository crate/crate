package io.crate.operator.operator;

import io.crate.metadata.FunctionInfo;
import org.cratedb.DataType;

public class LteOperator extends CmpOperator {

    public static final String NAME = "op_lte";

    public static void register(OperatorModule module) {
        for (DataType type : DataType.PRIMITIVE_TYPES) {
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
