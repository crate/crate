package io.crate.operation.scalar.arithmetic;

import com.google.common.collect.ImmutableMap;
import io.crate.data.Input;
import io.crate.metadata.FunctionInfo;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/*
 * Return the scale of the argument (the number of decimal digits in the fractional part)
 */
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
                .put(DataTypes.LONG, new FloatScaleFunction(Collections.singletonList(DataTypes.LONG)))
                .put(DataTypes.DOUBLE, new FloatScaleFunction(Collections.singletonList(DataTypes.DOUBLE)))
                .put(DataTypes.SHORT, new FloatScaleFunction(Collections.singletonList(DataTypes.SHORT)))
                .put(DataTypes.BYTE, new FloatScaleFunction(Collections.singletonList(DataTypes.BYTE)))
                .put(DataTypes.UNDEFINED, new FloatScaleFunction(Collections.singletonList(DataTypes.UNDEFINED)))
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
            if (!(value instanceof Float || value instanceof Double)) {
                return 0;
            }

            String numberAsString = String.format(Locale.ENGLISH, "%s", value);
            int dotLocation = numberAsString.indexOf('.');
            if (dotLocation != -1) {
                int numDigits = numberAsString.length() - dotLocation - 1;
                if (numDigits == 1 && numberAsString.charAt(numberAsString.length() - 1) == '0') {
                    return 0;
                }
                return numDigits;
            }
            return 0;
        }
    }
}
