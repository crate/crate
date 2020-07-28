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

package io.crate.analyze.relations;

import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.RelationName;
import io.crate.planner.node.dql.join.JoinType;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class RelationAnalysisContext {

    private final ExpressionAnalysisContext expressionAnalysisContext;
    private final boolean aliasedRelation;
    private final ParentRelations parents;
    // keep order of sources.
    //  e.g. something like:  select * from t1, t2 must not become select t2.*, t1.*
    private final Map<RelationName, AnalyzedRelation> sources = new LinkedHashMap<>();

    @Nullable
    private List<JoinPair> joinPairs;

    RelationAnalysisContext(boolean aliasedRelation,
                            ParentRelations parents) {
        this.aliasedRelation = aliasedRelation;
        this.parents = parents;
        this.expressionAnalysisContext = new ExpressionAnalysisContext();
    }

    boolean isAliasedRelation() {
        return aliasedRelation;
    }

    public Map<RelationName, AnalyzedRelation> sources() {
        return sources;
    }

    void addJoinPair(JoinPair joinType) {
        if (joinPairs == null) {
            joinPairs = new ArrayList<>();
        }
        joinPairs.add(joinType);
    }

    void addJoinType(JoinType joinType, @Nullable Symbol joinCondition) {
        int size = sources.size();
        assert size >= 2 : "sources must be added first, cannot add join type for only 1 source";
        Iterator<RelationName> it = sources.keySet().iterator();
        RelationName left = null;
        RelationName right = null;
        int idx = 0;
        while (it.hasNext()) {
            RelationName sourceName = it.next();
            if (idx == size - 2) {
                left = sourceName;
            } else if (idx == size - 1) {
                right = sourceName;
            }
            idx++;
        }
        addJoinPair(JoinPair.of(left, right, joinType, joinCondition));
    }

    List<JoinPair> joinPairs() {
        if (joinPairs == null) {
            return List.of();
        }
        return joinPairs;
    }

    void addSourceRelation(AnalyzedRelation relation) {
        RelationName relationName = relation.relationName();
        if (sources.put(relationName, relation) != null) {
            String errorMessage = String.format(Locale.ENGLISH, "\"%s\" specified more than once in the FROM clause", relationName);
            throw new IllegalArgumentException(errorMessage);
        }
    }

    public ExpressionAnalysisContext expressionAnalysisContext() {
        return expressionAnalysisContext;
    }

    public ParentRelations parentSources() {
        return parents;
    }
}
