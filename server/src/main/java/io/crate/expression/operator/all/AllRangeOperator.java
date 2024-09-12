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
 * WARRANTIES OR CONDITIONS OF ALL KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.expression.operator.all;


import org.apache.lucene.search.Query;

import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.lucene.LuceneQueryBuilder;
import io.crate.metadata.Reference;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.sql.tree.ComparisonExpression;

public final class AllRangeOperator extends AllOperator<Object> {

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

    AllRangeOperator(Signature signature, BoundSignature boundSignature, Comparison comparison) {
        super(signature, boundSignature);
        this.comparison = comparison;
    }

    @Override
    boolean matches(Object probe, Object candidate) {
        return comparison.isMatch(leftType.compare(probe, candidate));
    }

    @Override
    protected Query refMatchesAllArrayLiteral(Function all, Reference probe, Literal<?> literal, LuceneQueryBuilder.Context context) {
        return null;
    }

    @Override
    protected Query literalMatchesAllArrayRef(Function all, Literal<?> probe, Reference candidates, LuceneQueryBuilder.Context context) {
        return null;
    }
}
