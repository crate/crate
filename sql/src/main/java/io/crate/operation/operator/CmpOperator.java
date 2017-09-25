package io.crate.operation.operator;

import io.crate.core.collections.MapComparator;
import io.crate.data.Input;
import io.crate.metadata.BaseFunctionResolver;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.functions.params.FuncParams;
import io.crate.metadata.functions.params.Param;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

public abstract class CmpOperator extends Operator<Object> {

    /**
     * called inside {@link #normalizeSymbol(io.crate.analyze.symbol.Function)}
     * in order to interpret the result of compareTo
     * <p>
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
    public Boolean evaluate(Input<Object>... args) {
        assert args != null : "args must not be null";
        assert args.length == 2 : "number of args must be 2";
        assert args[0] != null && args[1] != null : "1st and 2nd argument must not be null";

        Object left = args[0].value();
        Object right = args[1].value();
        if (left == null || right == null) {
            return null;
        }

        assert (left.getClass().equals(right.getClass())) : "left and right must have the same type for comparison";

        if (left instanceof Comparable) {
            return compare(((Comparable) left).compareTo(right));
        } else if (left instanceof Map) {
            return compare(Objects.compare((Map) left, (Map) right, MapComparator.getInstance()));
        } else {
            return null;
        }
    }

    protected static class CmpResolver extends BaseFunctionResolver {

        private static final Param primitiveTypes = Param.of(DataTypes.PRIMITIVE_TYPES);

        private final String name;
        private final Function<FunctionInfo, FunctionImplementation> functionFactory;

        CmpResolver(String name, Function<FunctionInfo, FunctionImplementation> functionFactory) {
            this(name, FuncParams.builder(primitiveTypes, primitiveTypes).build(), functionFactory);
        }

        CmpResolver(String name,
                    FuncParams funcParams,
                    Function<FunctionInfo, FunctionImplementation> functionFactory) {
            super(funcParams);
            this.name = name;
            this.functionFactory = functionFactory;
        }

        @Override
        public FunctionImplementation getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
            FunctionInfo info = createInfo(name, dataTypes);
            return functionFactory.apply(info);
        }


        protected static FunctionInfo createInfo(String name, List<DataType> dataTypes) {
            return new FunctionInfo(new FunctionIdent(name, dataTypes), DataTypes.BOOLEAN);
        }

    }

}
