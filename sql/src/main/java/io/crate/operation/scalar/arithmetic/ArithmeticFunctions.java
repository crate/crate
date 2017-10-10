package io.crate.operation.scalar.arithmetic;

import com.google.common.collect.Sets;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.BaseFunctionResolver;
import io.crate.metadata.functions.params.FuncParams;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.metadata.functions.params.Param;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.DoubleType;
import io.crate.types.IntegerType;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.function.BinaryOperator;

public class ArithmeticFunctions {

    private static final Set<DataType> NUMERIC_WITH_DECIMAL = Sets.newHashSet(DataTypes.FLOAT, DataTypes.DOUBLE);

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
            (arg0, arg1) -> arg0 + arg1,
            (arg0, arg1) -> arg0 + arg1
        ));
        module.register(Names.SUBTRACT, new ArithmeticFunctionResolver(
            Names.SUBTRACT,
            "-",
            FunctionInfo.DETERMINISTIC_ONLY,
            (arg0, arg1) -> arg0 - arg1,
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
            (arg0, arg1) -> arg0 * arg1,
            (arg0, arg1) -> arg0 * arg1
        ));
        module.register(Names.DIVIDE, new ArithmeticFunctionResolver(
            Names.DIVIDE,
            "/",
            FunctionInfo.DETERMINISTIC_ONLY,
            (arg0, arg1) -> arg0 / arg1,
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
            (arg0, arg1) -> arg0 % arg1,
            (arg0, arg1) -> arg0 % arg1
        ));
        module.register(Names.POWER, new DoubleFunctionResolver(
            Names.POWER,
            (arg0, arg1) -> Math.pow(arg0, arg1)
        ));
    }

    static final class DoubleFunctionResolver extends BaseFunctionResolver {

        private static final Param ARITHMETIC_TYPE = Param.of(
            DataTypes.NUMERIC_PRIMITIVE_TYPES, DataTypes.TIMESTAMP);

        private final String name;
        private final BinaryOperator<Double> doubleFunction;

        DoubleFunctionResolver(String name, BinaryOperator<Double> doubleFunction) {
            super(FuncParams.builder(ARITHMETIC_TYPE, ARITHMETIC_TYPE).build());
            this.name = name;
            this.doubleFunction = doubleFunction;
        }

        @Override
        public FunctionImplementation getForTypes(List<DataType> args) throws IllegalArgumentException {
            return new BinaryScalar<>(doubleFunction, name, DataTypes.DOUBLE, FunctionInfo.DETERMINISTIC_ONLY);
        }
    }


    static final class ArithmeticFunctionResolver extends BaseFunctionResolver {

        private static final Param ARITHMETIC_TYPE = Param.of(
            DataTypes.NUMERIC_PRIMITIVE_TYPES, DataTypes.TIMESTAMP);

        private final String name;
        private final String operator;
        private final Set<FunctionInfo.Feature> features;

        private final BinaryOperator<Double> doubleFunction;
        private final BinaryOperator<Integer> integerFunction;
        private final BinaryOperator<Long> longFunction;
        private final BinaryOperator<Float> floatFunction;

        ArithmeticFunctionResolver(String name,
                                   String operator,
                                   Set<FunctionInfo.Feature> features,
                                   BinaryOperator<Integer> integerFunction,
                                   BinaryOperator<Double> doubleFunction,
                                   BinaryOperator<Long> longFunction,
                                   BinaryOperator<Float> floatFunction) {
            super(FuncParams.builder(ARITHMETIC_TYPE, ARITHMETIC_TYPE).build());
            this.name = name;
            this.operator = operator;
            this.doubleFunction = doubleFunction;
            this.integerFunction = integerFunction;
            this.longFunction = longFunction;
            this.floatFunction = floatFunction;
            this.features = features;
        }

        @Override
        public FunctionImplementation getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
            BinaryScalar<?> scalar;
            if (containsTypesWithDecimal(dataTypes)) {
                if (containsType(DoubleType.INSTANCE, dataTypes)) {
                    scalar = new BinaryScalar<>(doubleFunction, name, DataTypes.DOUBLE, features);
                } else {
                    scalar = new BinaryScalar<>(floatFunction, name, DataTypes.FLOAT, features);
                }
            } else {
                if (containsType(IntegerType.INSTANCE, dataTypes)) {
                    scalar = new BinaryScalar<>(integerFunction, name, DataTypes.INTEGER, features);
                } else {
                    scalar = new BinaryScalar<>(longFunction, name, DataTypes.LONG, features);
                }
            }
            return Scalar.withOperator(scalar, operator);
        }

    }

    public static Function of(String name, Symbol first, Symbol second, Set<FunctionInfo.Feature> features) {
        List<DataType> dataTypes = Arrays.asList(first.valueType(), second.valueType());
        if (containsTypesWithDecimal(dataTypes)) {
            return new Function(
                genDecimalInfo(name, dataTypes, features),
                Arrays.asList(first, second));
        }
        return new Function(
            genIntInfo(name, dataTypes, features),
            Arrays.asList(first, second));
    }

    static boolean containsTypesWithDecimal(List<DataType> dataTypes) {
        for (DataType dataType : dataTypes) {
            if (NUMERIC_WITH_DECIMAL.contains(dataType)) {
                return true;
            }
        }
        return false;
    }

    static boolean containsType(DataType type, List<DataType> dataTypes) {
        for (DataType dataType : dataTypes) {
            if (dataType.equals(type)) {
                return true;
            }
        }
        return false;
    }

    private static FunctionInfo genDecimalInfo(String functionName,
                                               List<DataType> dataTypes,
                                               Set<FunctionInfo.Feature> features) {
        return new FunctionInfo(
            new FunctionIdent(functionName, dataTypes),
            dataTypes.get(0),
            FunctionInfo.Type.SCALAR,
            features);
    }

    private static FunctionInfo genIntInfo(String functionName,
                                           List<DataType> dataTypes,
                                           Set<FunctionInfo.Feature> features) {
        return new FunctionInfo(
            new FunctionIdent(functionName, dataTypes),
            dataTypes.get(0),
            FunctionInfo.Type.SCALAR,
            features);
    }
}
