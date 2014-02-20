package io.crate.operator.operator;

import io.crate.metadata.FunctionInfo;
import io.crate.operator.Input;
import io.crate.planner.symbol.*;
import org.cratedb.core.collections.MapComparator;

import java.util.Map;

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

        if (containsNull(left, right)) {
            return Null.INSTANCE;
        }

        // TODO: implicit cast numeric literals

        if (left.symbolType().isLiteral() && right.symbolType().isLiteral()) {
            // must be true due to the function registration (argument DataType signature)
            return new BooleanLiteral(compare(((Literal) left).compareTo(right)));
        }

        return symbol;
    }

    @Override
    public Boolean evaluate(Input<Object>... args) {
        assert (args != null);
        assert (args.length == 2);

        if (args[0] == null && args[1] == null) {
            return compare(0);
        } else if (args[0] == null && args[1] != null) {
            return compare(1);
        } else if (args[0] != null && args[1] == null) {
            return compare(-1);
        }

        Object left = args[0].value();
        Object right = args[1].value();
        assert (left.getClass().equals(right.getClass()));

        if (left instanceof Comparable) {
            return compare(((Comparable)left).compareTo(right));
        } else if (left instanceof Map) {
            return compare(MapComparator.compareMaps((Map)left, (Map)right));
        }
        throw new UnsupportedOperationException("Could not compare the expressions");
    }

}
