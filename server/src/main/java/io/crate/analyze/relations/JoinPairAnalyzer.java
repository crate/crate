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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import io.crate.analyze.JoinRelation;
import io.crate.analyze.QueriedSelectRelation;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.RelationName;
import io.crate.planner.consumer.RelationNameCollector;

public class JoinPairAnalyzer extends AnalyzedRelationVisitor<JoinPairAnalyzer.Context, Void> {

    record Context(List<JoinPair> joinPair, List<RelationName> relationNames) {};

    public static List<JoinPair> apply(AnalyzedRelation analyzedRelation) {
        JoinPairAnalyzer joinPairAnalyzer = new JoinPairAnalyzer();
        JoinPairAnalyzer.Context context = new JoinPairAnalyzer.Context(new ArrayList<>(), new ArrayList<>());
        analyzedRelation.accept(joinPairAnalyzer, context);
        return context.joinPair;
    }

    @Override
    public Void visitAliasedAnalyzedRelation(AliasedAnalyzedRelation relation, Context context) {
        context.relationNames.add(relation.relationName());
        return null;
    }

    @Override
    protected Void visitAnalyzedRelation(AnalyzedRelation relation, Context context) {
        context.relationNames.add(relation.relationName());
        return null;
    }

    @Override
    public Void visitUnionSelect(UnionSelect unionSelect, Context context) {
        unionSelect.left().accept(this, context);
        unionSelect.right().accept(this, context);
        return null;
    }

    @Override
    public Void visitQueriedSelectRelation(QueriedSelectRelation relation, Context context) {
        for (AnalyzedRelation analyzedRelation : relation.from()) {
            analyzedRelation.accept(this, context);
        }
        return null;
    }

    @Override
    public Void visitJoinRelation(JoinRelation joinRelation, Context context) {
        joinRelation.left().accept(this, context);
        joinRelation.right().accept(this, context);

        RelationName left;
        RelationName right;
        Symbol joinCondition = joinRelation.joinCondition();
        if (joinCondition == null) {
            // This is a cross join with no join condition
            // like select * from t1 cross join t2
            left = joinRelation.left().relationName();
            right = joinRelation.right().relationName();
            context.joinPair.add(JoinPair.of(left, right, joinRelation.joinType(), null));
            return null;
        } else {
            // Now create the Join Pair in the original order of the query
            List<RelationName> sourceNames = new ArrayList<>(context.relationNames);
            Set<RelationName> relationNamesFromJoinConditions = RelationNameCollector.collect(joinCondition);
            // Only take the relation names into account which are part of the join condition
            sourceNames.retainAll(relationNamesFromJoinConditions);

            if (sourceNames.size() == 2) {
                //Join conditions like  on t1.x = t2.x for inner/outer/left/right
                left = sourceNames.get(0);
                right = sourceNames.get(1);
            } else if (sourceNames.size() > 2) {
                // we have three or more remaining relation names, this join is nested and
                // we have to take the side into account
                if (joinRelation.left() instanceof JoinRelation) {
                    left = sourceNames.get(sourceNames.size() - 2);
                    right = sourceNames.get(sourceNames.size() - 1);
                } else {
                    left = sourceNames.get(sourceNames.size() - 1);
                    right = sourceNames.get(sourceNames.size() - 2);
                }
            } else {
                // only one valid relation name in the join condition, this
                // is a boolean condition like on t.x is not null
                // this is unested, let's use directly left and right
                left = joinRelation.left().relationName();
                right = joinRelation.right().relationName();
            }
            context.joinPair.add(JoinPair.of(left, right, joinRelation.joinType(), joinCondition));
            return null;
        }
    }
}
