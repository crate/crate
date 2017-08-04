package io.crate.operation.scalar.arithmetic;

import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.*;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.function.BinaryOperator;

public class ArithmeticFunctions {

    public static class Names {
        public static final String ADD = "add";
        public static final String SUBTRACT = "subtract";
        public static final String MULTIPLY = "multiply";
        public static final String DIVIDE = "divide";
        public static final String POWER = "power";
        public static final String MODULUS = "modulus";
    }

    public static void register(ScalarFunctionModule module) {
        module.register(Names.ADD, new ArithmeticFunctionResolver(
            Names.ADD,
            "+",
            FunctionInfo.DETERMINISTIC_AND_COMPARISON_REPLACEMENT,
            (arg0, arg1) -> arg0 + arg1,
            (arg0, arg1) -> arg0 + arg1,
            (arg0, arg1) -> arg0 + arg1
        ));
        module.register(Names.SUBTRACT, new ArithmeticFunctionResolver(
            Names.SUBTRACT,
            "-",
            FunctionInfo.DETERMINISTIC_ONLY,
            (arg0, arg1) -> arg0 - arg1,
            (arg0, arg1) -> arg0 - arg1,
            (arg0, arg1) -> arg0 - arg1
        ));
        module.register(Names.MULTIPLY, new ArithmeticFunctionResolver(
            Names.MULTIPLY,
            "*",
            FunctionInfo.DETERMINISTIC_ONLY,
            (arg0, arg1) -> arg0 * arg1,
            (arg0, arg1) -> arg0 * arg1,
            (arg0, arg1) -> arg0 * arg1
        ));
        module.register(Names.DIVIDE, new ArithmeticFunctionResolver(
            Names.DIVIDE,
            "/",
            FunctionInfo.DETERMINISTIC_ONLY,
            (arg0, arg1) -> arg0 / arg1,
            (arg0, arg1) -> arg0 / arg1,
            (arg0, arg1) -> arg0 / arg1
        ));
        module.register(Names.MODULUS, new ArithmeticFunctionResolver(
            Names.MODULUS,
            "%",
            FunctionInfo.DETERMINISTIC_ONLY,
            (arg0, arg1) -> arg0 % arg1,
            (arg0, arg1) -> arg0 % arg1,
            (arg0, arg1) -> arg0 % arg1
        ));
        module.register(Names.POWER, new DoubleFunctionResolver(
            Names.POWER,
            (arg0, arg1) -> Math.pow(arg0, arg1)
        ));
    }

    final static class DoubleFunctionResolver extends BaseFunctionResolver {

        private static final Signature.ArgMatcher ARITHMETIC_TYPE = Signature.ArgMatcher.of(
            DataTypes.NUMERIC_PRIMITIVE_TYPES::contains, DataTypes.TIMESTAMP::equals);
        private final String name;
        private final BinaryOperator<Double> doubleFunction;

        DoubleFunctionResolver(String name, BinaryOperator<Double> doubleFunction) {
            super(Signature.of(ARITHMETIC_TYPE, ARITHMETIC_TYPE));
            this.name = name;
            this.doubleFunction = doubleFunction;
        }

        @Override
        public FunctionImplementation getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
            return new BinaryScalar<>(doubleFunction, name, DataTypes.DOUBLE, FunctionInfo.DETERMINISTIC_ONLY);
        }
    }


    final static class ArithmeticFunctionResolver extends BaseFunctionResolver {

        private static final Signature.ArgMatcher ARITHMETIC_TYPE = Signature.ArgMatcher.of(
            DataTypes.NUMERIC_PRIMITIVE_TYPES::contains, DataTypes.TIMESTAMP::equals);
        private final String name;
        private final String operator;
        private final Set<FunctionInfo.Feature> features;

        private final BinaryOperator<Double> doubleFunction;
        private final BinaryOperator<Long> longFunction;
        private final BinaryOperator<Float> floatFunction;

        ArithmeticFunctionResolver(String name,
                                   String operator,
                                   Set<FunctionInfo.Feature> features,
                                   BinaryOperator<Double> doubleFunction,
                                   BinaryOperator<Long> longFunction,
                                   BinaryOperator<Float> floatFunction) {
            super(Signature.of(ARITHMETIC_TYPE, ARITHMETIC_TYPE));
            this.name = name;
            this.operator = operator;
            this.doubleFunction = doubleFunction;
            this.longFunction = longFunction;
            this.floatFunction = floatFunction;
            this.features = features;
        }

        @Override
        public FunctionImplementation getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
            BinaryScalar<?> scalar;
            if (containsTypesWithDecimal(dataTypes)) {
                if (containsDouble(dataTypes)) {
                    scalar = new BinaryScalar<>(doubleFunction, name, DataTypes.DOUBLE, features);
                } else {
                    scalar = new BinaryScalar<>(floatFunction, name, DataTypes.FLOAT, features);
                }
            } else {
                scalar = new BinaryScalar<>(longFunction, name, DataTypes.LONG, features);
            }
            return Scalar.withOperator(scalar, operator);
        }

    }

    public static Function of(String name, Symbol first, Symbol second, Set<FunctionInfo.Feature> features) {
        List<DataType> argumentTypes = Arrays.asList(first.valueType(), second.valueType());
        if (containsTypesWithDecimal(argumentTypes)) {
            return new Function(
                genDoubleInfo(name, argumentTypes, features),
                Arrays.asList(first, second));
        }
        return new Function(
            genLongInfo(name, argumentTypes, features),
            Arrays.asList(first, second));
    }

    static boolean containsTypesWithDecimal(List<DataType> dataTypes) {
        for (DataType dataType : dataTypes) {
            if (DataTypes.NUMERIC_WITH_DECIMAL.contains(dataType)) {
                return true;
            }
        }
        return false;
    }

    static boolean containsDouble(List<DataType> dataTypes) {
        for (DataType dataType : dataTypes) {
            if (dataType.equals(DataTypes.DOUBLE)) {
                return true;
            }
        }
        return false;
    }

    static FunctionInfo genDoubleInfo(String functionName, List<DataType> dataTypes, Set<FunctionInfo.Feature> features) {
        return new FunctionInfo(new FunctionIdent(functionName, dataTypes), DataTypes.DOUBLE, FunctionInfo.Type.SCALAR, features);
    }

    static FunctionInfo genLongInfo(String functionName, List<DataType> dataTypes, Set<FunctionInfo.Feature> features) {
        return new FunctionInfo(new FunctionIdent(functionName, dataTypes), DataTypes.LONG, FunctionInfo.Type.SCALAR, features);
    }
}
