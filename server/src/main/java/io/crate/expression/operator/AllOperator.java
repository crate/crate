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

import static io.crate.metadata.functions.TypeVariableConstraint.typeVariable;

import java.util.Collection;
import java.util.function.IntPredicate;

import io.crate.data.Input;
import io.crate.metadata.NodeContext;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.sql.tree.ComparisonExpression;
import io.crate.types.DataType;
import io.crate.types.TypeSignature;

public final class AllOperator extends Operator<Object> {

    public static final String OPERATOR_PREFIX = "_all_";

    private enum Type {
        EQ(ComparisonExpression.Type.EQUAL, result -> result == 0),
        NEQ(ComparisonExpression.Type.NOT_EQUAL, result -> result != 0),
        GTE(ComparisonExpression.Type.GREATER_THAN_OR_EQUAL, result -> result >= 0),
        GT(ComparisonExpression.Type.GREATER_THAN, result -> result > 0),
        LTE(ComparisonExpression.Type.LESS_THAN_OR_EQUAL, result -> result <= 0),
        LT(ComparisonExpression.Type.LESS_THAN, result -> result < 0);

        final String fullQualifiedName;
        final IntPredicate cmp;

        Type(ComparisonExpression.Type type, IntPredicate cmp) {
            this.fullQualifiedName = OPERATOR_PREFIX + type.getValue();
            this.cmp = cmp;
        }
    }

    public static void register(OperatorModule module) {
        for (var type : Type.values()) {
            module.register(
                Signature.scalar(
                    type.fullQualifiedName,
                    TypeSignature.parse("E"),
                    TypeSignature.parse("array(E)"),
                    Operator.RETURN_TYPE.getTypeSignature()
                ).withTypeVariableConstraints(typeVariable("E")),
                (signature, boundSignature) ->
                    new AllOperator(
                        signature,
                        boundSignature,
                        type.cmp
                    )
            );
        }
    }

    private final IntPredicate cmp;
    private final DataType leftType;

    public AllOperator(Signature signature, BoundSignature boundSignature, IntPredicate cmp) {
        super(signature, boundSignature);
        this.cmp = cmp;
        this.leftType = boundSignature.argTypes().get(0);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Boolean evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<Object>[] args) {
        var leftValue = args[0].value();
        var rightValues = (Collection) args[1].value();
        if (leftValue == null || rightValues == null) {
            return null;
        }
        boolean anyNulls = false;
        int matches = 0;
        for (Object rightValue : rightValues) {
            if (rightValue == null) {
                anyNulls = true;
                matches++;
            } else if (cmp.test(leftType.compare(leftValue, rightValue))) {
                matches++;
            }
        }
        if (matches == rightValues.size()) {
            return anyNulls ? null : true;
        } else {
            return false;
        }
    }
}
