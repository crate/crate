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

public class ArrayAppendFunction extends Scalar<List<Object>, Object> {

    public static final String NAME = "array_append";

    public static void register(Functions.Builder builder) {
        builder.add(
                Signature.builder(NAME, FunctionType.SCALAR)
                        .argumentTypes(TypeSignature.parse("array(E)"),
                                TypeSignature.parse("E"))
                        .returnType(TypeSignature.parse("array(E)"))
                        .typeVariableConstraints(typeVariable("E"))
                        .features(Feature.DETERMINISTIC, Feature.NON_NULLABLE)
                        .build(),
                ArrayAppendFunction::new
        );
    }

    private final DataType<?> innerType;

    ArrayAppendFunction(Signature signature, BoundSignature boundSignature) {
        super(signature, boundSignature);
        this.innerType = ((ArrayType<?>) boundSignature.returnType()).innerType();
    }

    @Override
    public final List<Object> evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input[] args) {
        ArrayList<Object> resultList = new ArrayList<>();
        @SuppressWarnings("unchecked")
        List<Object> values = (List<Object>) args[0].value();
        Object valueToAdd = args[1].value();
        if (values != null) {
            for (Object value : values) {
                resultList.add(innerType.sanitizeValue(value));
            }
        }
        resultList.add(innerType.sanitizeValue(valueToAdd));
        return resultList;
    }
}
