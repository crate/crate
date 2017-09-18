package io.crate.operation.operator;

import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.data.Input;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.TransactionContext;
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
    public Symbol normalizeSymbol(Function function, TransactionContext transactionContext) {
        assert function != null : "function must not be null";
        assert function.arguments().size() == 2  : "number of args must be 2";

        Symbol left = function.arguments().get(0);
        Symbol right = function.arguments().get(1);

        if (left instanceof Input && right instanceof Input) {
            return Literal.of(evaluate((Input) left, (Input) right));
        }

        /*
         * true  or x    -> true
         * false or x    -> x
         * null  or x    -> null or true -> return function as is
         */
        if (left instanceof Input) {
            Object value = ((Input) left).value();
            if (value == null) {
                return function;
            }
            assert value instanceof Boolean : "value must be Boolean";
            if ((Boolean) value) {
                return Literal.of(true);
            } else {
                return right;
            }
        }

        if (right instanceof Input) {
            Object value = ((Input) right).value();
            if (value == null) {
                return function;
            }
            assert value instanceof Boolean : "value must be Boolean";
            if ((Boolean) value) {
                return Literal.of(true);
            } else {
                return left;
            }
        }

        return function;
    }

    @Override
    public Boolean evaluate(Input<Boolean>... args) {
        assert args != null : "args must not be null";
        assert args.length == 2 : "number of args must be 2";
        assert args[0] != null && args[1] != null : "1st and 2nd argument must not be null";

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
