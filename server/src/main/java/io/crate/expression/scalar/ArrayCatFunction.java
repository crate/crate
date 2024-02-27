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
import java.util.List;

import io.crate.data.Input;
import io.crate.metadata.Functions;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.TypeSignature;

public class ArrayCatFunction extends Scalar<List<Object>, List<Object>> {

    public static final String NAME = "array_cat";

    public static void register(Functions.Builder module) {
        module.add(
            Signature.scalar(
                NAME,
                TypeSignature.parse("array(E)"),
                TypeSignature.parse("array(E)"),
                TypeSignature.parse("array(E)")
            ).withFeature(Feature.NON_NULLABLE)
                .withTypeVariableConstraints(typeVariable("E")),
            ArrayCatFunction::new
        );
    }

    ArrayCatFunction(Signature signature, BoundSignature boundSignature) {
        super(signature, boundSignature);
        ensureBothInnerTypesAreNotUndefined(boundSignature.argTypes(), signature.getName().name());
    }

    @SafeVarargs
    @Override
    public final List<Object> evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<List<Object>>... args) {
        DataType<?> innerType = ((ArrayType<?>) this.boundSignature.returnType()).innerType();
        ArrayList<Object> resultList = new ArrayList<>();
        for (Input<List<Object>> arg : args) {
            List<Object> values = arg.value();
            if (values == null) {
                continue;
            }
            for (Object value : values) {
                resultList.add(innerType.sanitizeValue(value));
            }
        }
        return resultList;
    }

}
