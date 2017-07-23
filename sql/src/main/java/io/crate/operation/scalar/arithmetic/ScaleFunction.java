package io.crate.operation.scalar.arithmetic;

import com.google.common.collect.ImmutableMap;
import io.crate.data.Input;
import io.crate.metadata.FunctionInfo;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public abstract class ScaleFunction extends SingleArgumentArithmeticFunction {

    public static final String NAME = "scale";

    ScaleFunction(FunctionInfo info) {
        super(info);
    }

    public static void register(ScalarFunctionModule module) {
        Map<DataType, SingleArgumentArithmeticFunction> functionMap =
            ImmutableMap.<DataType, SingleArgumentArithmeticFunction>builder()
                .put(DataTypes.FLOAT, new FloatScaleFunction(Collections.singletonList(DataTypes.FLOAT)))
                .put(DataTypes.INTEGER, new FloatScaleFunction(Collections.singletonList(DataTypes.INTEGER)))
                .put(DataTypes.LONG, new DoubleScaleFunction(Collections.singletonList(DataTypes.LONG)))
                .put(DataTypes.DOUBLE, new DoubleScaleFunction(Collections.singletonList(DataTypes.DOUBLE)))
                .put(DataTypes.SHORT, new DoubleScaleFunction(Collections.singletonList(DataTypes.SHORT)))
                .put(DataTypes.BYTE, new DoubleScaleFunction(Collections.singletonList(DataTypes.BYTE)))
                .put(DataTypes.UNDEFINED, new DoubleScaleFunction(Collections.singletonList(DataTypes.UNDEFINED)))
                .build();
        module.register(NAME, new Resolver(NAME, functionMap));
    }

    private static class FloatScaleFunction extends ScaleFunction {

        FloatScaleFunction(List<DataType> dataTypes) {
            super(generateFloatFunctionInfo(NAME, dataTypes));
        }

        @Override
        public Integer evaluate(Input[] args) {
            Object value = args[0].value();
            if (value == null) {
                return null;
            }

            String numberAsString = value.toString();
            if (numberAsString.contains("-")) {
                numberAsString.replace("-", "");
            }
            if (numberAsString.contains(".")) {
                int integerPlaces = numberAsString.indexOf('.');
                return numberAsString.length() - integerPlaces - 1;
            }
            return 0;
        }
    }


    private static class DoubleScaleFunction extends ScaleFunction {

        DoubleScaleFunction(List<DataType> dataTypes) {
            super(generateDoubleFunctionInfo(NAME, dataTypes));
        }

        @Override
        public Long evaluate(Input[] args) {
            Object value = args[0].value();
            if (value == null) {
                return null;
            }
            String numberAsString = value.toString();
            if (numberAsString.contains("-")) {
                numberAsString.replace("-", "");
            }
            if (numberAsString.contains(".")) {
                int integerPlaces = numberAsString.indexOf('.');
                return new Long(numberAsString.length() - integerPlaces - 1);
            }
            return new Long(0);

        }
    }

}
