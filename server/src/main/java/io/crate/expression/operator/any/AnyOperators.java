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

package io.crate.expression.operator.any;

import io.crate.common.annotations.VisibleForTesting;
import io.crate.common.collections.Lists2;
import io.crate.expression.operator.LikeOperators;
import io.crate.expression.operator.Operator;
import io.crate.expression.operator.OperatorModule;
import io.crate.metadata.functions.Signature;
import io.crate.sql.tree.ComparisonExpression;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.function.IntPredicate;

import static io.crate.expression.operator.any.AnyOperator.OPERATOR_PREFIX;
import static io.crate.metadata.functions.TypeVariableConstraint.typeVariable;
import static io.crate.types.TypeSignature.parseTypeSignature;

public final class AnyOperators {

    public static final List<String> OPERATOR_NAMES = Lists2.concat(
        AnyOperators.Type.fullQualifiedNames(),
        List.of(
            LikeOperators.ANY_LIKE,
            LikeOperators.ANY_ILIKE,
            LikeOperators.ANY_NOT_LIKE,
            LikeOperators.ANY_NOT_ILIKE
        )
    );

    public enum Type {
        EQ(ComparisonExpression.Type.EQUAL, result -> result == 0),
        NEQ(ComparisonExpression.Type.NOT_EQUAL, result -> result != 0),
        GTE(ComparisonExpression.Type.GREATER_THAN_OR_EQUAL, result -> result >= 0),
        GT(ComparisonExpression.Type.GREATER_THAN, result -> result > 0),
        LTE(ComparisonExpression.Type.LESS_THAN_OR_EQUAL, result -> result <= 0),
        LT(ComparisonExpression.Type.LESS_THAN, result -> result < 0);

        final String opName;
        final String opSymbol;
        final IntPredicate cmp;

        Type(ComparisonExpression.Type type, IntPredicate cmp) {
            this.opName = OPERATOR_PREFIX + type.getValue();
            this.opSymbol = type.getValue();
            this.cmp = cmp;
        }

        public String opName() {
            return opName;
        }

        public static List<String> fullQualifiedNames() {
            return Lists2.map(Arrays.asList(values()), t -> t.opName);
        }

        @VisibleForTesting
        public static List<String> operatorSymbols() {
            return Lists2.map(Arrays.asList(values()), t -> t.opSymbol);
        }
    }

    public static void register(OperatorModule module) {
        for (var type : Type.values()) {
            module.register(
                Signature.scalar(
                    type.opName,
                    parseTypeSignature("E"),
                    parseTypeSignature("array(E)"),
                    Operator.RETURN_TYPE.getTypeSignature()
                ).withTypeVariableConstraints(typeVariable("E")),
                (signature, boundSignature) ->
                    new AnyOperator(
                        signature,
                        boundSignature,
                        type.cmp
                    )
            );
        }
    }

    public static Iterable<?> collectionValueToIterable(Object collectionRef) throws IllegalArgumentException {
        if (collectionRef instanceof Object[]) {
            return Arrays.asList((Object[]) collectionRef);
        } else if (collectionRef instanceof Collection) {
            return (Collection<?>) collectionRef;
        } else {
            throw new IllegalArgumentException(
                String.format(Locale.ENGLISH, "cannot cast %s to Iterable", collectionRef));
        }
    }

    private AnyOperators() {
    }
}
