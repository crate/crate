package io.crate.operator.operator;

import com.google.common.base.Preconditions;
import io.crate.metadata.FunctionInfo;
import io.crate.operator.Input;
import io.crate.planner.symbol.*;
import org.cratedb.DataType;

public class OrOperator extends Operator {

    public static final String NAME = "op_or";
    public static final FunctionInfo INFO = generateInfo(NAME, DataType.BOOLEAN);

    public static void register(OperatorModule module) {
        module.registerOperatorFunction(new OrOperator());
    }

    @Override
    public FunctionInfo info() {
        return INFO;
    }

    @Override
    public Symbol normalizeSymbol(Function function) {
        Preconditions.checkNotNull(function);

        for (Symbol symbol : function.arguments()) {
            if (symbol.symbolType() == SymbolType.BOOLEAN_LITERAL) {
                if (((BooleanLiteral)symbol).value()) {
                    return new BooleanLiteral(true);
                }
            }
        }

        return function;
    }

    @Override
    public Boolean evaluate(Input<?>... args) {
        if (args == null) {
            return false;
        }
        for (Input<?> input : args) {
            if ((Boolean)input.value()) {
                return true;
            }
        }
        return false;
    }
}
