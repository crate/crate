/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.operation.scalar.arithmetic;

import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.format.OperatorFormatSpec;
import io.crate.metadata.*;
import io.crate.data.Input;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.types.DataType;

import java.util.List;

public abstract class DivideFunction extends ArithmeticFunction implements OperatorFormatSpec {

    public static final String NAME = "divide";
    private static final String SQL_SYMBOL = "/";

    public static void register(ScalarFunctionModule module) {
        module.register(NAME, new Resolver());
    }

    DivideFunction(FunctionInfo info) {
        super(info);
    }

    @Override
    public String operator(Function function) {
        return SQL_SYMBOL;
    }

    private static class DoubleDivideFunction extends DivideFunction {

        DoubleDivideFunction(FunctionInfo info) {
            super(info);
        }

        @Override
        public Number evaluate(Input[] args) {
            assert args.length == 2 : "number of args must be 2";
            Object arg0Value = args[0].value();
            Object arg1Value = args[1].value();

            if (arg0Value == null) {
                return null;
            }
            if (arg1Value == null) {
                return null;
            }
            return ((Number) arg0Value).doubleValue() / ((Number) arg1Value).doubleValue();
        }
    }

    private static class FloatDivideFunction extends SubtractFunction {

        FloatDivideFunction(FunctionInfo info) {
            super(info);
        }

        @Override
        public Number evaluate(Input[] args) {
            assert args.length == 2 : "number of args must be 2";
            Object arg0Value = args[0].value();
            Object arg1Value = args[1].value();

            if (arg0Value == null) {
                return null;
            }
            if (arg1Value == null) {
                return null;
            }
            return ((Number) arg0Value).floatValue() / ((Number) arg1Value).floatValue();
        }
    }

    private static class LongDivideFunction extends DivideFunction {

        LongDivideFunction(FunctionInfo info) {
            super(info);
        }

        @Override
        public Number evaluate(Input[] args) {
            assert args.length == 2 : "number of args must be 2";
            Object arg0Value = args[0].value();
            Object arg1Value = args[1].value();

            if (arg0Value == null) {
                return null;
            }
            if (arg1Value == null) {
                return null;
            }
            return ((Number) arg0Value).longValue() / ((Number) arg1Value).longValue();
        }
    }


    private static class Resolver extends ArithmeticFunctionResolver {

        @Override
        public FunctionImplementation getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
            if (containsTypesWithDecimal(dataTypes)) {
                if (containsDouble(dataTypes)) {
                    return new DoubleDivideFunction(genDoubleInfo(NAME, dataTypes));
                }
                return new FloatDivideFunction(genFloatInfo(NAME, dataTypes));
            }
            return new LongDivideFunction(genLongInfo(NAME, dataTypes));
        }
    }
}
