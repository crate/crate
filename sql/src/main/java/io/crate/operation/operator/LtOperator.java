package io.crate.operation.operator;

import io.crate.metadata.FunctionInfo;

public class LtOperator extends CmpOperator {

    public static final String NAME = "op_<";

    public static void register(OperatorModule module) {
        module.registerDynamicOperatorFunction(NAME, new CmpResolver(NAME, LtOperator::new));
    }

    LtOperator(FunctionInfo info) {
        super(info);
    }

    @Override
    protected boolean compare(int comparisonResult) {
        return comparisonResult < 0;
    }
}
