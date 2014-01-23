package io.crate.operator.operator;

import com.google.common.collect.ImmutableList;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.planner.symbol.*;
import org.cratedb.DataType;

public class OrOperator implements Operator {

    public static final String NAME = "op_or";
    private final FunctionInfo info;

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

    @Override
    public FunctionInfo info() {
        return info;
    }

    @Override
    public Symbol normalizeSymbol(Function function) {

        for (Symbol symbol : function.arguments()) {
            if (symbol.symbolType() == SymbolType.BOOlEAN_LITERAL) {
                if (((BooleanLiteral)symbol).value()) {
                    return new BooleanLiteral(true);
                }
            }
        }

        return function;
    }
}
