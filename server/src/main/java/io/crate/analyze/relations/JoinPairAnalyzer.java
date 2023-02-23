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

import static io.crate.common.collections.Iterables.getOnlyElement;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import io.crate.analyze.JoinRelation;
import io.crate.analyze.QueriedSelectRelation;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.RelationName;
import io.crate.planner.consumer.RelationNameCollector;

public class JoinPairAnalyzer extends AnalyzedRelationVisitor<JoinPairAnalyzer.Context, Void> {

    record Context(List<JoinPair> joinPair, List<RelationName> relationNames, List<RelationName> sourceNames) {};

    public static List<JoinPair> apply(AnalyzedRelation analyzedRelation, List<RelationName> sourceNames) {
        JoinPairAnalyzer joinPairAnalyzer = new JoinPairAnalyzer();
        JoinPairAnalyzer.Context context = new JoinPairAnalyzer.Context(new ArrayList<>(), new ArrayList<>(), sourceNames);
        analyzedRelation.accept(joinPairAnalyzer, context);
        return context.joinPair;
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
        Symbol joinCondition = joinRelation.joinCondition();
        Set<RelationName> collect = RelationNameCollector.collect(joinCondition);
        RelationName left;
        RelationName right;
        if (collect.size() == 1) {
            left = joinRelation.left().relationName();
            right = joinRelation.right().relationName();
        } else {
            var it = collect.iterator();
            left = it.next();
            right = it.next();
        }
        // Now create the Join Pair in the original order of the query
        var leftIndex = context.sourceNames.indexOf(left);
        var rightIndex = context.sourceNames.indexOf(right);
        if (leftIndex > rightIndex) {
            var temp = left;
            left = right;
            right = temp;
        }
        context.joinPair.add(JoinPair.of(left, right, joinRelation.joinType(), joinCondition));
        return null;
    }
}
