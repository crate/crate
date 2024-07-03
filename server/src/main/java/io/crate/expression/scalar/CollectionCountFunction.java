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

public class CollectionCountFunction extends Scalar<Long, List<Object>> {

    public static final String NAME = "collection_count";

    public static void register(Functions.Builder builder) {
        builder.add(
                Signature.builder(NAME, FunctionType.SCALAR)
                        .argumentTypes(TypeSignature.parse("array(E)"))
                        .returnType(DataTypes.LONG.getTypeSignature())
                        .typeVariableConstraints(typeVariable("E"))
                        .features(Feature.DETERMINISTIC, Feature.NULLABLE)
                        .build(),
                CollectionCountFunction::new
        );
    }

    private CollectionCountFunction(Signature signature, BoundSignature boundSignature) {
        super(signature, boundSignature);
    }

    @Override
    @SafeVarargs
    public final Long evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<List<Object>>... args) {
        List<Object> argArray = args[0].value();
        if (argArray == null) {
            return null;
        }
        return (long) argArray.size();
    }
}
