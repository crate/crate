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

import static io.crate.metadata.functions.TypeVariableConstraint.typeVariable;

import java.util.ArrayList;
import java.util.List;

import io.crate.data.Input;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.FunctionType;
import io.crate.metadata.Functions;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.TypeSignature;

/**
 * <pre>
 *  array_unnest([[1, 2], [3, 4]]) -> [1, 2, 3, 4]
 * </pre>
 */
public class ArrayUnnestFunction extends Scalar<List<Object>, List<List<Object>>> {

    public static final String NAME = "array_unnest";
    public static final Signature SIGNATURE = Signature.builder(NAME, FunctionType.SCALAR)
            .argumentTypes(TypeSignature.parse("array(array(E))"))
            .returnType(TypeSignature.parse("array(E)"))
            .typeVariableConstraints(typeVariable("E"))
            .features(Feature.DETERMINISTIC, Feature.NULLABLE)
            .build();

    public static void register(Functions.Builder module) {
        module.add(SIGNATURE, ArrayUnnestFunction::new);
    }

    public static Symbol unnest(Symbol arg, int dimensions) {
        for (int i = 0; i < dimensions; i++) {
            DataType<?> valueType = arg.valueType();
            assert valueType instanceof ArrayType<?> : "Argument to unnest must be an array, not: " + valueType;
            DataType<?> returnType = ((ArrayType<?>) valueType).innerType();
            arg = new Function(SIGNATURE, List.of(arg), returnType);
        }
        return arg;
    }

    public ArrayUnnestFunction(Signature signature, BoundSignature boundSignature) {
        super(signature, boundSignature);
    }

    @Override
    @SafeVarargs
    public final List<Object> evaluate(TransactionContext txnCtx, NodeContext nodeContext, Input<List<List<Object>>>... args) {
        List<List<Object>> values = args[0].value();
        if (values == null) {
            return null;
        }
        ArrayList<Object> result = new ArrayList<>();
        for (List<Object> innerValues : values) {
            if (innerValues != null) {
                result.addAll(innerValues);
            }
        }
        return result;
    }
}
