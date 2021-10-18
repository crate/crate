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


import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;

import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.expression.operator.CmpOperator;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.lucene.LuceneQueryBuilder.Context;
import io.crate.metadata.Reference;
import io.crate.metadata.functions.Signature;
import io.crate.sql.tree.ComparisonExpression;

public final class AnyRangeOperator extends AnyOperator {

    public enum Comparison {
        GT(ComparisonExpression.Type.GREATER_THAN, ComparisonExpression.Type.LESS_THAN) {

            @Override
            boolean isMatch(int compareToResult) {
                return compareToResult > 0;
            }
        },
        GTE(ComparisonExpression.Type.GREATER_THAN_OR_EQUAL, ComparisonExpression.Type.LESS_THAN_OR_EQUAL) {

            @Override
            boolean isMatch(int compareToResult) {
                return compareToResult >= 0;
            }
        },
        LT(ComparisonExpression.Type.LESS_THAN, ComparisonExpression.Type.GREATER_THAN) {

            @Override
            boolean isMatch(int compareToResult) {
                return compareToResult < 0;
            }
        },
        LTE(ComparisonExpression.Type.LESS_THAN_OR_EQUAL, ComparisonExpression.Type.GREATER_THAN_OR_EQUAL) {

            @Override
            boolean isMatch(int compareToResult) {
                return compareToResult <= 0;
            }

        };

        private final String innerOpName;
        private final String opName;
        private final String inverseInnerOpName;
        private final String symbol;

        private Comparison(ComparisonExpression.Type cmpExpressionType, ComparisonExpression.Type inverseComparison) {
            this.symbol = cmpExpressionType.getValue();
            this.innerOpName = "op_" + symbol;
            this.opName = OPERATOR_PREFIX + symbol;
            this.inverseInnerOpName = "op_" + inverseComparison.getValue();
        }

        public String opName() {
            return opName;
        }

        abstract boolean isMatch(int compareToResult);
    }

    private final Comparison comparison;

    AnyRangeOperator(Signature signature, Signature boundSignature, Comparison comparison) {
        super(signature, boundSignature);
        this.comparison = comparison;
    }

    @Override
    boolean matches(Object probe, Object candidate) {
        return comparison.isMatch(leftType.compare(probe, candidate));
    }

    @Override
    protected Query refMatchesAnyArrayLiteral(Function any, Reference probe, Literal<?> candidates, Context context) {
        // col < ANY ([1,2,3]) --> or(col<1, col<2, col<3)
        BooleanQuery.Builder booleanQuery = new BooleanQuery.Builder();
        booleanQuery.setMinimumNumberShouldMatch(1);
        for (Object value : (Iterable<?>) candidates.value()) {
            booleanQuery.add(
                CmpOperator.toQuery(comparison.innerOpName, probe, value, context),
                BooleanClause.Occur.SHOULD);
        }
        return booleanQuery.build();
    }

    @Override
    protected Query literalMatchesAnyArrayRef(Function any, Literal<?> probe, Reference candidates, Context context) {
        // 1 < ANY (array_col) --> array_col > 1
        try {
            return CmpOperator.toQuery(comparison.inverseInnerOpName, candidates, probe.value(), context);
        } catch (IllegalArgumentException | ClassCastException ex) {
            throw new UnsupportedFeatureException("Cannot use `" + comparison.symbol + " ANY` if left side is an array");
        }
    }
}
