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

package io.crate.operation.scalar;

import io.crate.metadata.*;
import io.crate.operation.Input;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Symbol;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;

import java.util.List;
import java.util.Locale;

public abstract class ConcatFunction extends Scalar<BytesRef, BytesRef> {

    public static final String NAME = "concat";
    private static final BytesRef EMPTY_STRING = new BytesRef("");
    private FunctionInfo functionInfo;

    public static void register(ScalarFunctionModule module) {
        module.register(NAME, new Resolver());
    }

    protected ConcatFunction(FunctionInfo functionInfo) {
        this.functionInfo = functionInfo;
    }

    @Override
    public FunctionInfo info() {
        return functionInfo;
    }

    @Override
    public Symbol normalizeSymbol(Function function) {
        if (anyNonLiterals(function.arguments())) {
            return function;
        }
        Input[] inputs = new Input[function.arguments().size()];
        for (int i = 0; i < function.arguments().size(); i++) {
            inputs[i] = ((Input) function.arguments().get(i));
        }
        //noinspection unchecked
        return Literal.newLiteral(functionInfo.returnType(), evaluate(inputs));
    }

    private static class StringConcatFunction extends ConcatFunction {

        protected StringConcatFunction(FunctionInfo functionInfo) {
            super(functionInfo);
        }

        @Override
        public BytesRef evaluate(Input[] args) {
            BytesRef firstArg = (BytesRef) args[0].value();
            BytesRef secondArg = (BytesRef) args[1].value();
            if (firstArg == null) {
                if (secondArg == null) {
                    return EMPTY_STRING;
                }
                return secondArg;
            }
            if (secondArg == null) {
                return firstArg;
            }
            byte[] bytes = new byte[firstArg.length + secondArg.length];
            System.arraycopy(firstArg.bytes, firstArg.offset, bytes, 0, firstArg.length);
            System.arraycopy(secondArg.bytes, secondArg.offset, bytes, firstArg.length, secondArg.length);
            return new BytesRef(bytes);
        }
    }

    private static class GenericConcatFunction extends ConcatFunction {

        protected GenericConcatFunction(FunctionInfo functionInfo) {
            super(functionInfo);
        }

        @Override
        public BytesRef evaluate(Input[] args) {
            BytesRef[] bytesRefs = new BytesRef[args.length];
            int numBytes = 0;
            for (int i = 0; i < args.length; i++) {
                Input input = args[i];
                BytesRef value = DataTypes.STRING.value(input.value());
                if (value == null) {
                    value = EMPTY_STRING;
                }
                bytesRefs[i] = value;
                numBytes += value.length;
            }

            byte[] bytes = new byte[numBytes];
            int numWritten = 0;
            for (BytesRef bytesRef : bytesRefs) {
                System.arraycopy(bytesRef.bytes, bytesRef.offset, bytes, numWritten, bytesRef.length);
                numWritten += bytesRef.length;
            }
            return new BytesRef(bytes);
        }
    }

    private static class Resolver implements DynamicFunctionResolver {

        @Override
        public FunctionImplementation<Function> getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
            if (dataTypes.size() < 2) {
                throw new IllegalArgumentException("concat function requires at least 2 arguments");
            } else if (dataTypes.size() == 2 && dataTypes.get(0).equals(DataTypes.STRING) && dataTypes.get(1).equals(DataTypes.STRING)) {
                return new StringConcatFunction(new FunctionInfo(new FunctionIdent(NAME, dataTypes), DataTypes.STRING));
            } else {
                for (int i = 0; i < dataTypes.size(); i++) {
                    if (!dataTypes.get(i).isConvertableTo(DataTypes.STRING)) {
                        throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                                "Argument %d of the concat function can't be converted to string", i));
                    }
                }
                return new GenericConcatFunction(new FunctionInfo(new FunctionIdent(NAME, dataTypes), DataTypes.STRING));
            }
        }
    }
}
