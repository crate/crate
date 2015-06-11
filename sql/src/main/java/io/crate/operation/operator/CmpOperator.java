package io.crate.operation.operator;

import io.crate.core.collections.MapComparator;
import io.crate.metadata.FunctionInfo;
import io.crate.operation.Input;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Symbol;

import java.util.Map;
import java.util.Objects;

public abstract class CmpOperator extends Operator<Object> {

    /**
     * called inside {@link #normalizeSymbol(io.crate.planner.symbol.Function)}
     * in order to interpret the result of compareTo
     *
     * subclass has to implement this to evaluate the -1, 0, 1 to boolean
     * e.g. for Lt  -1 is true, 0 and 1 is false.
     *
     * @param comparisonResult the result of someLiteral.compareTo(otherLiteral)
     * @return true/false
     */
    protected abstract boolean compare(int comparisonResult);
    protected FunctionInfo info;

    protected CmpOperator(FunctionInfo info) {
        this.info = info;
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Symbol normalizeSymbol(Function symbol) {
        assert (symbol != null);
        assert (symbol.arguments().size() == 2);

        Symbol left = symbol.arguments().get(0);
        Symbol right = symbol.arguments().get(1);

        if (containsNullLiteral(symbol.arguments())) {
            return Literal.NULL;
        }

        if (left.symbolType().isValueSymbol() && right.symbolType().isValueSymbol()) {
            // must be true due to the function registration (argument DataType signature)
            if (compare(((Literal) left).compareTo((Literal) right))){
                return Literal.BOOLEAN_TRUE;
            } else {
                return Literal.BOOLEAN_FALSE;
            }
        }

        return symbol;
    }

    @Override
    public Boolean evaluate(Input<Object>... args) {
        assert (args != null);
        assert (args.length == 2);
        assert (args[0] != null && args[1] != null);

        Object left = args[0].value();
        Object right = args[1].value();
        if (left == null || right == null) {
            return null;
        }

        assert (left.getClass().equals(right.getClass())) : "left and right must have the same type for comparison";

        if (left instanceof Comparable) {
            return compare(((Comparable)left).compareTo(right));
        } else if (left instanceof Map) {
            return compare(Objects.compare((Map)left, (Map)right, MapComparator.getInstance()));
        } else {
            return null;
        }
    }

}
