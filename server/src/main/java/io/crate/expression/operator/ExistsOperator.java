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

package io.crate.expression.operator;

import java.util.List;

import io.crate.data.Input;
import io.crate.metadata.NodeContext;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.metadata.functions.TypeVariableConstraint;
import io.crate.types.TypeSignature;

public class ExistsOperator extends Operator<List<Object>> {

    public static final String NAME = "_exists";

    public static void register(OperatorModule module) {
        Signature signature = Signature.scalar(
                NAME,
                TypeSignature.parse("array(E)"),
                Operator.RETURN_TYPE.getTypeSignature()
            ).withTypeVariableConstraints(TypeVariableConstraint.typeVariable("E"))
            .withFeature(Feature.NULLABLE);
        module.register(signature, ExistsOperator::new);
    }

    private ExistsOperator(Signature signature, BoundSignature boundSignature) {
        super(signature, boundSignature);
    }

    @Override
    @SafeVarargs
    public final Boolean evaluate(TransactionContext txnCtx, NodeContext nodeContext, Input<List<Object>>... args) {
        List<Object> value = args[0].value();
        if (value == null) {
            return null;
        }
        return !value.isEmpty();
    }
}
