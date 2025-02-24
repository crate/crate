/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

import static io.crate.metadata.functions.TypeVariableConstraint.typeVariable;

import java.util.Collections;
import java.util.List;

import io.crate.data.Input;
import io.crate.metadata.FunctionType;
import io.crate.metadata.Functions;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataTypes;
import io.crate.types.TypeSignature;

public class ArrayOverlapFunction extends Scalar<Boolean, List<Object>> {

    public static final String NAME = "array_overlap";

    public static final Signature SIGNATURE = Signature.builder(NAME, FunctionType.SCALAR)
        .argumentTypes(TypeSignature.parse("array(E)"),
            TypeSignature.parse("array(E)"))
        .returnType(DataTypes.BOOLEAN.getTypeSignature())
        .typeVariableConstraints(typeVariable("E"))
        .features(Scalar.Feature.DETERMINISTIC, Scalar.Feature.NOTNULL)
        .build();

    public static void register(Functions.Builder module) {
        module.add(SIGNATURE, ArrayOverlapFunction::new);
    }

    ArrayOverlapFunction(Signature signature, BoundSignature boundSignature) {
        super(signature, boundSignature);
    }

    @Override
    @SafeVarargs
    public final Boolean evaluate(TransactionContext txnCtx, NodeContext nodeContext, Input<List<Object>>... args) {
        List<Object> firstList = args[0].value();
        List<Object> secondList = args[1].value();
        if (firstList == null || secondList == null) {
            return null;
        }
        return !Collections.disjoint(firstList, secondList);
    }
}
