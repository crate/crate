package io.crate.operation.operator;

import io.crate.metadata.FunctionInfo;
import io.crate.operation.Input;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Symbol;
import io.crate.types.DataTypes;

public class OrOperator extends Operator<Boolean> {

    public static final String NAME = "op_or";
    public static final FunctionInfo INFO = generateInfo(NAME, DataTypes.BOOLEAN);

    public static void register(OperatorModule module) {
        module.registerOperatorFunction(new OrOperator());
    }

    @Override
    public FunctionInfo info() {
        return INFO;
    }

    @Override
    public Symbol normalizeSymbol(Function function) {
        assert (function != null);
        assert function.arguments().size() == 2;

        Symbol left = function.arguments().get(0);
        Symbol right = function.arguments().get(1);

        if (left.symbolType().isLiteral() && right.symbolType().isLiteral()) {
            return Literal.newLiteral(evaluate((Input) left, (Input) right));
        }

        /***
         * true  or x    -> true
         * false or x    -> x
         * null  or x    -> null or true -> return function as is
         */
        if (left instanceof Input) {
            Object value = ((Input) left).value();
            if (value == null) {
                return function;
            }
            assert value instanceof Boolean;
            if ((Boolean) value) {
                return Literal.newLiteral(true);
            } else {
                return right;
            }
        }

        if (right instanceof Input) {
            Object value = ((Input) right).value();
            if (value == null) {
                return function;
            }
            assert value instanceof Boolean;
            if ((Boolean) value) {
                return Literal.newLiteral(true);
            } else {
                return left;
            }
        }

        return function;
    }

    @Override
    public Boolean evaluate(Input<Boolean>... args) {
        assert (args != null);
        assert (args.length == 2);
        assert (args[0] != null && args[1] != null);

        // implement three valued logic.
        // don't touch anything unless you have a good reason for it! :)
        // http://en.wikipedia.org/wiki/Three-valued_logic
        Boolean left = args[0].value();
        Boolean right = args[1].value();

        if (left == null && right == null) {
            return null;
        }

        if (left == null) {
            return (right) ? true : null;
        }

        if (right == null) {
            return (left) ? true : null;
        }

        return left || right;
    }

}
