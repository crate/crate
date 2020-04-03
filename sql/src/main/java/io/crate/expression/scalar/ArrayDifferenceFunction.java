/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.expression.scalar;

import io.crate.data.Input;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.Signature;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static io.crate.expression.scalar.array.ArrayArgumentValidators.ensureBothInnerTypesAreNotUndefined;
import static io.crate.metadata.functions.TypeVariableConstraint.typeVariable;
import static io.crate.types.TypeSignature.parseTypeSignature;

class ArrayDifferenceFunction extends Scalar<List<Object>, List<Object>> {

    public static final String NAME = "array_difference";

    public static void register(ScalarFunctionModule module) {
        module.register(
            Signature.scalar(
                NAME,
                parseTypeSignature("array(E)"),
                parseTypeSignature("array(E)"),
                parseTypeSignature("array(E)")
            ).withTypeVariableConstraints(typeVariable("E")),
            (signature, argumentTypes) ->
                new ArrayDifferenceFunction(
                    createInfo(argumentTypes),
                    signature,
                    null
                )
        );
    }

    private static FunctionInfo createInfo(List<DataType> types) {
        ensureBothInnerTypesAreNotUndefined(types, NAME);
        ArrayType<?> arrayType = (ArrayType<?>) types.get(0);
        if (arrayType.innerType().equals(DataTypes.UNDEFINED)) {
            arrayType = (ArrayType<?>) types.get(1);
        }
        return new FunctionInfo(new FunctionIdent(NAME, types), arrayType);
    }

    private final FunctionInfo functionInfo;
    private final Signature signature;
    private final Optional<Set<Object>> optionalSubtractSet;

    private ArrayDifferenceFunction(FunctionInfo functionInfo,
                                    Signature signature,
                                    @Nullable Set<Object> subtractSet) {
        this.functionInfo = functionInfo;
        this.signature = signature;
        optionalSubtractSet = Optional.ofNullable(subtractSet);
    }

    @Override
    public FunctionInfo info() {
        return functionInfo;
    }

    @Nullable
    @Override
    public Signature signature() {
        return signature;
    }

    @Override
    public Scalar<List<Object>, List<Object>> compile(List<Symbol> arguments) {
        Symbol symbol = arguments.get(1);

        if (!symbol.symbolType().isValueSymbol()) {
            // arguments are no values, we can't compile
            return this;
        }

        Input<?> input = (Input<?>) symbol;
        Object inputValue = input.value();

        DataType<?> innerType = ((ArrayType<?>) this.info().returnType()).innerType();
        List<Object> values = (List<Object>) inputValue;
        Set<Object> subtractSet;
        if (values == null) {
            subtractSet = Collections.emptySet();
        } else {
            subtractSet = new HashSet<>();
            for (Object element : values) {
                subtractSet.add(innerType.value(element));
            }
        }
        return new ArrayDifferenceFunction(this.functionInfo, signature, subtractSet);
    }

    @Override
    public List<Object> evaluate(TransactionContext txnCtx, Input[] args) {
        List<Object> inputValues = (List<Object>) args[0].value();
        if (inputValues == null) {
            return null;
        }

        DataType<?> innerType = ((ArrayType<?>) this.info().returnType()).innerType();
        Set<Object> localSubtractSet;
        if (optionalSubtractSet.isEmpty()) {
            localSubtractSet = new HashSet<>();
            for (int i = 1; i < args.length; i++) {
                Object argValue = args[i].value();
                if (argValue == null) {
                    continue;
                }

                List<Object> values = (List<Object>) argValue;
                for (Object element : values) {
                    localSubtractSet.add(innerType.value(element));
                }
            }
        } else {
            localSubtractSet = optionalSubtractSet.get();
        }

        ArrayList<Object> resultList = new ArrayList<>(inputValues.size());
        for (Object value : inputValues) {
            if (!localSubtractSet.contains(innerType.value(value))) {
                resultList.add(value);
            }
        }
        return resultList;
    }
}
