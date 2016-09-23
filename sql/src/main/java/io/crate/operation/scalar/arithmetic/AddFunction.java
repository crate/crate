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
import io.crate.metadata.DynamicFunctionResolver;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionInfo;
import io.crate.operation.Input;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.types.DataType;

import java.util.List;

public abstract class AddFunction extends ArithmeticFunction implements OperatorFormatSpec {

    public static final String NAME = "add";
    public static final String SQL_SYMBOL = "+";

    public AddFunction(FunctionInfo info) {
        super(info);
    }

    public static void register(ScalarFunctionModule module) {
        module.register(NAME, new Resolver());
    }

    @Override
    public String operator(Function function) {
        return SQL_SYMBOL;
    }

    private static class DoubleAddFunction extends AddFunction {

        public DoubleAddFunction(FunctionInfo info) {
            super(info);
        }

        @Override
        public Number evaluate(Input[] args) {
            assert args.length == 2;
            Object arg0Value = args[0].value();
            Object arg1Value = args[1].value();

            if (arg0Value == null) {
                return null;
            }
            if (arg1Value == null) {
                return null;
            }
            return ((Number) arg0Value).doubleValue() + ((Number) arg1Value).doubleValue();
        }
    }

    private static class LongAddFunction extends AddFunction {

        public LongAddFunction(FunctionInfo info) {
            super(info);
        }

        @Override
        public Number evaluate(Input[] args) {
            assert args.length == 2;
            Object arg0Value = args[0].value();
            Object arg1Value = args[1].value();

            if (arg0Value == null) {
                return null;
            }
            if (arg1Value == null) {
                return null;
            }
            return ((Number) arg0Value).longValue() + ((Number) arg1Value).longValue();
        }
    }

    private static class Resolver implements DynamicFunctionResolver {

        @Override
        public FunctionImplementation<Function> getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
            validateTypes(dataTypes);
            if (containsTypesWithDecimal(dataTypes)) {
                return new DoubleAddFunction(genDoubleInfo(NAME, dataTypes, true));
            }
            return new LongAddFunction(genLongInfo(NAME, dataTypes, true));
        }
    }
}
