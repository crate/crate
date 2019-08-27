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

package io.crate.expression.scalar;

import com.google.common.base.Preconditions;
import io.crate.data.Input;
import io.crate.exceptions.ConversionException;
import io.crate.expression.symbol.FuncArg;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.FunctionResolver;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.params.FuncParams;
import io.crate.metadata.functions.params.Param;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Locale;

public abstract class ConcatFunction extends Scalar<String, String> {

    public static final String NAME = "concat";
    private FunctionInfo functionInfo;

    public static void register(ScalarFunctionModule module) {
        module.register(NAME, new Resolver());
    }

    ConcatFunction(FunctionInfo functionInfo) {
        this.functionInfo = functionInfo;
    }

    @Override
    public FunctionInfo info() {
        return functionInfo;
    }

    @Override
    public Symbol normalizeSymbol(Function function, TransactionContext txnCtx) {
        if (anyNonLiterals(function.arguments())) {
            return function;
        }
        Input[] inputs = new Input[function.arguments().size()];
        for (int i = 0; i < function.arguments().size(); i++) {
            inputs[i] = ((Input) function.arguments().get(i));
        }
        //noinspection unchecked
        return Literal.of(functionInfo.returnType(), evaluate(txnCtx, inputs));
    }

    private static class StringConcatFunction extends ConcatFunction {

        StringConcatFunction(FunctionInfo functionInfo) {
            super(functionInfo);
        }

        @Override
        public String evaluate(TransactionContext txnCtx, Input[] args) {
            String firstArg = (String) args[0].value();
            String secondArg = (String) args[1].value();
            if (firstArg == null) {
                if (secondArg == null) {
                    return "";
                }
                return secondArg;
            }
            if (secondArg == null) {
                return firstArg;
            }
            return firstArg + secondArg;
        }
    }

    private static class GenericConcatFunction extends ConcatFunction {

        GenericConcatFunction(FunctionInfo functionInfo) {
            super(functionInfo);
        }

        @Override
        public String evaluate(TransactionContext txnCtx, Input<String>[] args) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < args.length; i++) {
                String value = args[i].value();
                if (value != null) {
                    sb.append(value);
                }
            }
            return sb.toString();
        }
    }

    private static class Resolver implements FunctionResolver {

        private static final FuncParams STRINGS = FuncParams
            .builder(Param.STRING, Param.STRING)
            .withVarArgs(Param.STRING)
            .build();
        private static final FuncParams ARRAYS = FuncParams
            .builder(
                Param.of(new ArrayType<>(DataTypes.UNDEFINED)).withInnerType(Param.ANY),
                Param.of(new ArrayType<>(DataTypes.UNDEFINED)).withInnerType(Param.ANY)
            )
            .build();

        @Nullable
        @Override
        public List<DataType> getSignature(List<? extends FuncArg> funcArgs) {
            try {
                return STRINGS.match(funcArgs);
            } catch (ConversionException e1) {
                try {
                    return ARRAYS.match(funcArgs);
                } catch (ConversionException e2) {
                    throw e1; // prefer error message with casts to string instead of casts to arrays
                }
            }
        }

        @Override
        public FunctionImplementation getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
            if (dataTypes.size() == 2 && dataTypes.get(0).equals(DataTypes.STRING) &&
                       dataTypes.get(1).equals(DataTypes.STRING)) {
                return new StringConcatFunction(new FunctionInfo(new FunctionIdent(NAME, dataTypes), DataTypes.STRING));
            } else if (dataTypes.size() == 2 && dataTypes.get(0) instanceof ArrayType &&
                       dataTypes.get(1) instanceof ArrayType) {

                DataType innerType0 = ((ArrayType) dataTypes.get(0)).innerType();
                DataType innerType1 = ((ArrayType) dataTypes.get(1)).innerType();

                Preconditions.checkArgument(
                    !innerType0.equals(DataTypes.UNDEFINED) || !innerType1.equals(DataTypes.UNDEFINED),
                    "When concatenating arrays, one of the two arguments can be of undefined inner type, but not both");

                if (!innerType0.equals(DataTypes.UNDEFINED)) {
                    Preconditions.checkArgument(innerType1.isConvertableTo(innerType0),
                        String.format(Locale.ENGLISH,
                            "Second argument's inner type (%s) of the array_cat function cannot be converted to the first argument's inner type (%s)",
                            innerType1, innerType0));
                }

                return new ArrayCatFunction(ArrayCatFunction.createInfo(dataTypes));
            } else {
                return new GenericConcatFunction(new FunctionInfo(new FunctionIdent(NAME, dataTypes), DataTypes.STRING));
            }
        }
    }
}
