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

package io.crate.expression.operator.any;

import io.crate.expression.operator.OperatorModule;
import io.crate.sql.tree.ComparisonExpression;

import java.util.Arrays;
import java.util.Collection;
import java.util.Locale;

import static io.crate.expression.operator.any.AnyOperator.OPERATOR_PREFIX;
import static io.crate.expression.operator.any.AnyOperators.Names.EQ;
import static io.crate.expression.operator.any.AnyOperators.Names.GT;
import static io.crate.expression.operator.any.AnyOperators.Names.GTE;
import static io.crate.expression.operator.any.AnyOperators.Names.LT;
import static io.crate.expression.operator.any.AnyOperators.Names.LTE;
import static io.crate.expression.operator.any.AnyOperators.Names.NEQ;

public final class AnyOperators {

    private AnyOperators() {
    }

    public static class Names {
        public static final String EQ = OPERATOR_PREFIX + ComparisonExpression.Type.EQUAL.getValue();
        public static final String GTE = OPERATOR_PREFIX + ComparisonExpression.Type.GREATER_THAN_OR_EQUAL.getValue();
        public static final String GT = OPERATOR_PREFIX + ComparisonExpression.Type.GREATER_THAN.getValue();
        public static final String LTE = OPERATOR_PREFIX + ComparisonExpression.Type.LESS_THAN_OR_EQUAL.getValue();
        public static final String LT = OPERATOR_PREFIX + ComparisonExpression.Type.LESS_THAN.getValue();
        public static final String NEQ = OPERATOR_PREFIX + ComparisonExpression.Type.NOT_EQUAL.getValue();
    }

    public static void register(OperatorModule module) {
        module.registerDynamicOperatorFunction(EQ, new AnyOperator.AnyResolver(EQ, result -> result == 0));
        module.registerDynamicOperatorFunction(GTE, new AnyOperator.AnyResolver(GTE, result -> result >= 0));
        module.registerDynamicOperatorFunction(GT, new AnyOperator.AnyResolver(GT, result -> result > 0));
        module.registerDynamicOperatorFunction(LTE, new AnyOperator.AnyResolver(LTE, result -> result <= 0));
        module.registerDynamicOperatorFunction(LT, new AnyOperator.AnyResolver(LT, result -> result < 0));
        module.registerDynamicOperatorFunction(NEQ, new AnyOperator.AnyResolver(NEQ, result -> result != 0));
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
}
