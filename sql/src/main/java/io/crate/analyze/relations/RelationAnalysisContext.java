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

import com.google.common.collect.ImmutableList;
import io.crate.analyze.AnalysisMetaData;
import io.crate.analyze.ParameterContext;
import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.StmtCtx;
import io.crate.planner.node.dql.join.JoinType;
import io.crate.sql.tree.QualifiedName;

import javax.annotation.Nullable;
import java.util.*;

public class RelationAnalysisContext {

    private final ExpressionAnalysisContext expressionAnalysisContext;
    private final ParameterContext parameterContext;
    private final AnalysisMetaData analysisMetaData;
    private final boolean aliasedRelation;
    // keep order of sources.
    //  e.g. something like:  select * from t1, t2 must not become select t2.*, t1.*
    private final Map<QualifiedName, AnalyzedRelation> sources = new LinkedHashMap<>();

    private ExpressionAnalyzer expressionAnalyzer;
    private FieldProvider fieldProvider;

    @Nullable
    private List<Symbol> joinConditions;

    @Nullable
    private List<JoinPair> joinPairs;

    RelationAnalysisContext(ParameterContext parameterContext,
                            StmtCtx stmtCtx,
                            AnalysisMetaData analysisMetaData,
                            boolean aliasedRelation) {
        this.parameterContext = parameterContext;
        this.analysisMetaData = analysisMetaData;
        this.aliasedRelation = aliasedRelation;
        expressionAnalysisContext = new ExpressionAnalysisContext(stmtCtx);
    }

    boolean isAliasedRelation() {
        return aliasedRelation;
    }

    public Map<QualifiedName, AnalyzedRelation> sources() {
        return sources;
    }

    List<Symbol> joinConditions() {
        if (joinConditions == null) {
            return ImmutableList.of();
        }
        return joinConditions;
    }

    void addJoinCondition(Symbol symbol) {
        if (joinConditions == null) {
            joinConditions = new ArrayList<>();
        }
        joinConditions.add(symbol);
    }

    private void addJoinPair(JoinPair joinType) {
        if (joinPairs == null) {
            joinPairs = new ArrayList<>();
        }
        joinPairs.add(joinType);
    }

    void addJoinType(JoinType joinType, @Nullable Symbol joinCondition) {
        int size = sources.size();
        assert size >= 2 : "sources must be added first, cannot add join type for only 1 source";
        Iterator<QualifiedName> it = sources.keySet().iterator();
        QualifiedName left = null;
        QualifiedName right = null;
        int idx = 0;
        while (it.hasNext()) {
            QualifiedName sourceName = it.next();
            if (idx == size - 2) {
                left = sourceName;
            } else if (idx == size - 1) {
                right = sourceName;
            }
            idx++;
        }
        addJoinPair(new JoinPair(left, right, joinType, joinCondition));
    }

    List<JoinPair> joinPairs() {
        if (joinPairs == null) {
            return new ArrayList<>();
        }
        return joinPairs;
    }

    private void addSourceRelation(QualifiedName qualifiedName, AnalyzedRelation relation) {
        if (sources.put(qualifiedName, relation) != null) {
            String tableName = qualifiedName.toString();
            if (tableName.startsWith(".")) {
                tableName = tableName.substring(1);
            }
            String errorMessage = String.format(Locale.ENGLISH, "\"%s\" specified more than once in the FROM clause", tableName);
            throw new IllegalArgumentException(errorMessage);
        }
    }

    void addSourceRelation(String nameOrAlias, AnalyzedRelation relation) {
        addSourceRelation(new QualifiedName(nameOrAlias), relation);
    }

    void addSourceRelation(String schemaName, String nameOrAlias, AnalyzedRelation relation) {
        addSourceRelation(new QualifiedName(Arrays.asList(schemaName, nameOrAlias)), relation);
    }

    public ExpressionAnalyzer expressionAnalyzer() {
        if (expressionAnalyzer == null){
            expressionAnalyzer = new ExpressionAnalyzer(analysisMetaData, parameterContext, fieldProvider(), null);
        }
        return expressionAnalyzer;
    }

    public ExpressionAnalysisContext expressionAnalysisContext() {
        return expressionAnalysisContext;
    }

    public FieldProvider fieldProvider() {
        if (fieldProvider == null) {
            fieldProvider = new FullQualifedNameFieldProvider(sources());
        }
        return fieldProvider;
    }
}
