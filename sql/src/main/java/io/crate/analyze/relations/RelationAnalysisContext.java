/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

import com.google.common.base.Strings;
import io.crate.analyze.AnalysisMetaData;
import io.crate.analyze.ParameterContext;
import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.sql.tree.QualifiedName;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class RelationAnalysisContext {

    private final ExpressionAnalysisContext expressionAnalysisContext;
    private ExpressionAnalyzer expressionAnalyzer;
    private Map<QualifiedName, AnalyzedRelation> sources = new HashMap<>();
    private ParameterContext parameterContext;
    private AnalysisMetaData analysisMetaData;
    private FullQualifedNameFieldProvider fieldProvider;

    public RelationAnalysisContext(ParameterContext parameterContext, AnalysisMetaData analysisMetaData) {
        this.parameterContext = parameterContext;
        this.analysisMetaData = analysisMetaData;
        this.expressionAnalysisContext = new ExpressionAnalysisContext();
    }

    public ParameterContext parameterContext() {
        return parameterContext;
    }

    public void addSourceRelation(String nameOrAlias, AnalyzedRelation relation) {
        addSourceRelation(new QualifiedName(nameOrAlias), relation);
    }

    public void addSourceRelation(String schemaName, String nameOrAlias, AnalyzedRelation relation) {
        addSourceRelation(new QualifiedName(Arrays.asList(schemaName, nameOrAlias)), relation);
    }

    private void addSourceRelation(QualifiedName qualifiedName, AnalyzedRelation relation) {
        if (sources.put(qualifiedName, relation) != null ) {
            String tableName = qualifiedName.toString();
            if (tableName.startsWith(".")) {
                tableName = tableName.substring(1);
            }
            String errorMessage = String.format("\"%s\" specified more than once in the FROM clause", tableName);
            throw new IllegalArgumentException(errorMessage);
        }
    }

    public Map<QualifiedName, AnalyzedRelation> sources() {
        return sources;
    }

    public ExpressionAnalyzer expressionAnalyzer(){
        if (expressionAnalyzer == null){
            expressionAnalyzer = new ExpressionAnalyzer(analysisMetaData, parameterContext(),
                    fieldProvider());
        }
        return expressionAnalyzer;
    }

    FieldProvider fieldProvider(){
        if (fieldProvider == null){
            fieldProvider = new FullQualifedNameFieldProvider(sources());
        }
        return fieldProvider;
    }

    public ExpressionAnalysisContext expressionAnalysisContext() {
        return expressionAnalysisContext;
    }
}
