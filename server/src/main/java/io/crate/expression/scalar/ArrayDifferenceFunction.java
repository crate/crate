/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

import static io.crate.expression.scalar.array.ArrayArgumentValidators.ensureBothInnerTypesAreNotUndefined;
import static io.crate.metadata.functions.TypeVariableConstraint.typeVariable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.jetbrains.annotations.Nullable;

import io.crate.data.Input;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.TypeSignature;
import io.crate.role.Roles;

class ArrayDifferenceFunction extends Scalar<List<Object>, List<Object>> {

    public static final String NAME = "array_difference";

    public static void register(ScalarFunctionModule module) {
        module.register(
            Signature.scalar(
                    NAME,
                    TypeSignature.parse("array(E)"),
                    TypeSignature.parse("array(E)"),
                    TypeSignature.parse("array(E)")
                ).withTypeVariableConstraints(typeVariable("E"))
                .withFeature(Feature.NULLABLE),
            (signature, boundSignature) ->
                new ArrayDifferenceFunction(
                    signature,
                    boundSignature,
                    null
                )
        );
    }

    private final Optional<Set<Object>> optionalSubtractSet;

    private ArrayDifferenceFunction(Signature signature,
                                    BoundSignature boundSignature,
                                    @Nullable Set<Object> subtractSet) {
        super(signature, boundSignature);
        optionalSubtractSet = Optional.ofNullable(subtractSet);
        ensureBothInnerTypesAreNotUndefined(boundSignature.argTypes(), NAME);
    }

    @Override
    public Scalar<List<Object>, List<Object>> compile(List<Symbol> arguments, String currentUser, Roles roles) {
        Symbol symbol = arguments.get(1);

        if (!symbol.symbolType().isValueSymbol()) {
            // arguments are no values, we can't compile
            return this;
        }

        Input<?> input = (Input<?>) symbol;
        Object inputValue = input.value();

        DataType<?> innerType = ((ArrayType<?>) this.boundSignature.returnType()).innerType();
        List<Object> values = (List<Object>) inputValue;
        Set<Object> subtractSet;
        if (values == null) {
            subtractSet = Collections.emptySet();
        } else {
            subtractSet = new HashSet<>();
            for (Object element : values) {
                subtractSet.add(innerType.sanitizeValue(element));
            }
        }
        return new ArrayDifferenceFunction(signature, boundSignature, subtractSet);
    }

    @Override
    public List<Object> evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input[] args) {
        List<Object> inputValues = (List<Object>) args[0].value();
        if (inputValues == null) {
            return null;
        }

        DataType<?> innerType = ((ArrayType<?>) this.boundSignature.returnType()).innerType();
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
                    localSubtractSet.add(innerType.sanitizeValue(element));
                }
            }
        } else {
            localSubtractSet = optionalSubtractSet.get();
        }

        ArrayList<Object> resultList = new ArrayList<>(inputValues.size());
        for (Object value : inputValues) {
            if (!localSubtractSet.contains(innerType.sanitizeValue(value))) {
                resultList.add(value);
            }
        }
        return resultList;
    }
}
