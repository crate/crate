package io.crate.operator.operator;

import io.crate.metadata.FunctionInfo;
import org.cratedb.DataType;

public class GteOperator extends CmpOperator {

    public static final String NAME = "op_>=";

    public static void register(OperatorModule module) {
        for (DataType type : DataType.PRIMITIVE_TYPES) {
            module.registerOperatorFunction(new GteOperator(generateInfo(NAME, type)));
        }
    }

    GteOperator(FunctionInfo info) {
        super(info);
    }

    @Override
    protected boolean compare(int comparisonResult) {
        return comparisonResult >= 0;
    }
}
