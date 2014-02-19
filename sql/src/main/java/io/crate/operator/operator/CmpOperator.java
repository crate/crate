package io.crate.operator.operator;

import com.google.common.collect.ImmutableMap;
import io.crate.metadata.FunctionInfo;
import io.crate.operator.Input;
import io.crate.planner.symbol.*;
import org.cratedb.DataType;
import org.cratedb.core.collections.MapComparator;

import java.util.Comparator;
import java.util.Map;
import java.util.Objects;

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
            assert left.getClass() == right.getClass();
            return new BooleanLiteral(compare(((Literal) left).compareTo(right)));
        }

        return symbol;
    }

    @Override
    public Boolean evaluate(Input<?>... args) {
        assert (args != null);
        assert (args.length == 2);

        // handle Input.value() as Object to get the Class of an object.
        Object left = args[0].value();
        Object right = args[1].value();
        assert (left.getClass().equals(right.getClass()));

        if (!(left instanceof Comparable) && !(left instanceof Map) ||
            !(right instanceof Comparable) && !(right instanceof Map)) {
            throw new UnsupportedOperationException("Failed to evaluate. Arguments are not comparable");
        }

        Comparator comparator = lookupComparator(left);
        if (comparator == null) {
            throw new UnsupportedOperationException("Failed to evaluate. Unsupported data type");
        }
        return compare(Objects.compare(left, right, comparator));
    }

    protected static Comparator lookupComparator(Object left) {
        DataType dataType = DataType.forValue(left);
        return cmpMap.get(dataType);
    }

    private final static Map<DataType, Comparator> cmpMap = ImmutableMap.<DataType, Comparator>builder()
            .put(DataType.BYTE, new PrimitiveCmp())
            .put(DataType.SHORT, new PrimitiveCmp())
            .put(DataType.INTEGER, new PrimitiveCmp())
            .put(DataType.LONG, new PrimitiveCmp())
            .put(DataType.FLOAT, new PrimitiveCmp())
            .put(DataType.DOUBLE, new PrimitiveCmp())
            .put(DataType.BOOLEAN, new PrimitiveCmp())
            .put(DataType.STRING, new PrimitiveCmp())
            .put(DataType.OBJECT, new ObjectCmp())
            .build();
        /*
            no need for putting following data types:
                DataType.TIMESTAMP      // handled with LongCmp
                DataType.IP             // handled with StringCmp
        */

    protected static class PrimitiveCmp implements Comparator<Comparable> {
        @Override
        public int compare(Comparable o1, Comparable o2) {
            return o1.compareTo(o2);
        }
    }

    protected static class ObjectCmp implements Comparator<Map<String,Object>> {
        @Override
        public int compare(Map<String,Object> o1, Map<String,Object> o2) {
            return MapComparator.compareMaps(o1, o2);
        }
    }

}
