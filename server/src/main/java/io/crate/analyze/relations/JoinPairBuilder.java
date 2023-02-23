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

package io.crate.analyze.relations;

import java.util.List;
import java.util.Locale;
import java.util.Optional;

import io.crate.analyze.JoinRelation;
import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.exceptions.RelationUnknown;
import io.crate.exceptions.RelationValidationException;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.RelationName;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.JoinCriteria;
import io.crate.sql.tree.JoinOn;
import io.crate.sql.tree.JoinUsing;

public class JoinPairBuilder {

    public static List<JoinPair> buildJoinPair(JoinRelation joinRelation,
                                        ExpressionAnalyzer expressionAnalyzer,
                                        ExpressionAnalysisContext expressionAnalysisContext,
                                        List<RelationName> sourceNames) {
        Optional<JoinCriteria> optCriteria = joinRelation.joinCriteria();
        if (optCriteria.isEmpty()) {
            return List.of();
        }
        var joinCriteria = optCriteria.get();
        Expression expr = null;
        if (joinCriteria instanceof JoinOn joinOn) {
            expr = joinOn.getExpression();
        } else if (joinCriteria instanceof JoinUsing joinUsing) {
            // this will break on a nested join, when one of the underlying relations is e.g. another join or a union
            expr = JoinUsing.toExpression(
                joinRelation.left().relationName().toQualifiedName(),
                joinRelation.right().relationName().toQualifiedName(),
                joinUsing.getColumns());
        } else {
            throw new UnsupportedOperationException(
                String.format(Locale.ENGLISH, "join criteria %s not supported", joinCriteria.getClass().getSimpleName())
            );
        }
        try {
            Symbol joinCondition = expressionAnalyzer.convert(expr, expressionAnalysisContext);
            var relationNames = RelationNameExtractor.collect(joinRelation);
            if (relationNames.size() >= 2) {
                var it = relationNames.iterator();
                var left = it.next();
                var right = it.next();
                // Now create the Join Pair in the original order of the relations
                var leftIndex = sourceNames.indexOf(left);
                var rightIndex = sourceNames.indexOf(right);
                if (leftIndex > rightIndex) {
                    var temp = left;
                    left = right;
                    right = temp;
                }
                return List.of(JoinPair.of(left, right, joinRelation.joinType(), joinCondition));
            } else {
                throw new IllegalStateException();
            }
        } catch (RelationUnknown e) {
            throw new RelationValidationException(e.getTableIdents(),
                                                  String.format(Locale.ENGLISH,
                                                                "missing FROM-clause entry for relation '%s'",
                                                                e.getTableIdents()));
        }

    }
}
