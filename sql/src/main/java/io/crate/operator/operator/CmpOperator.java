package io.crate.operator.operator;

import com.google.common.base.Preconditions;
import io.crate.metadata.FunctionInfo;
import io.crate.planner.symbol.*;

public abstract class CmpOperator extends Operator {

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
        Preconditions.checkNotNull(symbol);
        Preconditions.checkArgument(symbol.arguments().size() == 2);

        Symbol left = symbol.arguments().get(0);
        Symbol right = symbol.arguments().get(1);


        if (containsNull(left, right)) {
            return Null.INSTANCE;
        }

        // TODO: implicit cast numeric literals

        if (left.symbolType().isLiteral() && right.symbolType().isLiteral()) {
            // must be true due to the function registration (argument DataType signature)
            assert left.getClass() == right.getClass();
            return new BooleanLiteral(compare(((Literal) left).compareTo(right)));
        }

        return symbol;
    }
}
