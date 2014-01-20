package io.crate.operator.operator;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.sun.org.apache.xalan.internal.xsltc.compiler.sym;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionInfo;
import io.crate.operator.Input;
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
    public Symbol optimizeSymbol(Symbol symbol) {
        Preconditions.checkArgument(symbol.symbolType() == SymbolType.FUNCTION);
        Function function = (Function) symbol;

        for (ValueSymbol valueSymbol : function.arguments()) {
            if (valueSymbol.symbolType() == SymbolType.BOOlEAN_LITERAL) {
                if (((BooleanLiteral)valueSymbol).value()) {
                    return new BooleanLiteral(true);
                }
            }
        }

        return symbol;
    }
}
