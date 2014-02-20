package io.crate.operator.operator;

import com.google.common.base.Preconditions;
import io.crate.metadata.FunctionInfo;
import io.crate.operator.Input;
import io.crate.planner.symbol.BooleanLiteral;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Symbol;
import io.crate.planner.symbol.SymbolType;
import org.cratedb.DataType;

public class OrOperator extends Operator<Boolean> {

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
    public Boolean evaluate(Input<Boolean>... args) {
        assert (args != null);
        assert (args.length == 2);

        // implement three valued logic.
        // don't touch anything unless you have a good reason for it! :)
        // http://en.wikipedia.org/wiki/Three-valued_logic
        Boolean left = (args[0] == null) ? null : args[0].value();
        Boolean right = (args[1] == null) ? null : args[1].value();

        if (left == null && right == null) {
            return null;
        }

        if (left == null) {
            if (right != null && right) {
                return true;
            } else {
                return null;
            }
        }

        if (right == null) {
            if (left != null && left) {
                return true;
            } else {
                return null;
            }
        }

        return left || right;
    }

}
