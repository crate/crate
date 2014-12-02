/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.analyze;

import com.google.common.collect.ImmutableMap;
import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.TableRelation;
import io.crate.exceptions.PartitionUnknownException;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.metadata.TableIdent;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.symbol.Field;
import io.crate.planner.symbol.StringValueSymbolVisitor;
import io.crate.planner.symbol.Symbol;
import io.crate.sql.tree.*;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class CopyStatementAnalyzer extends AbstractStatementAnalyzer<Void, CopyAnalyzedStatement> {

    private final AnalysisMetaData analysisMetaData;
    private ExpressionAnalysisContext expressionAnalysisContext;
    private ExpressionAnalyzer expressionAnalyzer;

    public CopyStatementAnalyzer(AnalysisMetaData analysisMetaData) {
        this.analysisMetaData = analysisMetaData;
    }

    @Override
    public Void visitCopyFromStatement(CopyFromStatement node, CopyAnalyzedStatement context) {
        process(node.table(), context);
        setExpressionAnalyzer(context);
        if (node.genericProperties().isPresent()) {
            context.settings(settingsFromProperties(node.genericProperties().get(), context));
        }
        context.mode(CopyAnalyzedStatement.Mode.FROM);

        if (!node.table().partitionProperties().isEmpty()) {
            context.partitionIdent(PartitionPropertiesAnalyzer.toPartitionIdent(
                            context.table(),
                            node.table().partitionProperties(),
                            context.parameters()));
        }

        Symbol pathSymbol = expressionAnalyzer.convert(node.path(), expressionAnalysisContext);
        context.uri(Field.unwrap(pathSymbol));
        return null;
    }

    @Override
    public Void visitCopyTo(CopyTo node, CopyAnalyzedStatement context) {
        context.mode(CopyAnalyzedStatement.Mode.TO);
        process(node.table(), context);
        setExpressionAnalyzer(context);
        if (node.genericProperties().isPresent()) {
            context.settings(settingsFromProperties(node.genericProperties().get(), context));
        }

        if (!node.table().partitionProperties().isEmpty()) {
            String partitionIdent = PartitionPropertiesAnalyzer.toPartitionIdent(
                    context.table(),
                    node.table().partitionProperties(),
                    context.parameters());
            if (!context.partitionExists(partitionIdent)){
                throw new PartitionUnknownException(context.table().ident().fqn(), partitionIdent);
            }
            context.partitionIdent(partitionIdent);
        }
        Symbol uri = expressionAnalyzer.convert(node.targetUri(), expressionAnalysisContext);
        context.uri(Field.unwrap(uri));
        context.directoryUri(node.directoryUri());

        List<Symbol> columns = new ArrayList<>(node.columns().size());
        for (Expression expression : node.columns()) {
            columns.add(Field.unwrap(expressionAnalyzer.convert(expression, expressionAnalysisContext)));
        }
        context.outputSymbols(columns);
        return null;
    }

    private void setExpressionAnalyzer(CopyAnalyzedStatement context) {
        TableInfo tableInfo = context.table();
        expressionAnalyzer = new ExpressionAnalyzer(
                analysisMetaData,
                context.parameterContext(),
                ImmutableMap.<QualifiedName, AnalyzedRelation>of(
                        new QualifiedName(Arrays.asList(tableInfo.schemaInfo().name(), tableInfo.ident().name())),
                        new TableRelation(tableInfo)));
        if (context.mode() == CopyAnalyzedStatement.Mode.FROM) {
            expressionAnalyzer.resolveWritableFields(true);
        }
        expressionAnalysisContext = new ExpressionAnalysisContext();
    }

    private Settings settingsFromProperties(GenericProperties properties, CopyAnalyzedStatement context) {
        ImmutableSettings.Builder builder = ImmutableSettings.builder();
        for (Map.Entry<String, Expression> entry : properties.properties().entrySet()) {
            String key = entry.getKey();
            Expression expression = entry.getValue();
            if (expression instanceof ArrayLiteral) {
                throw new IllegalArgumentException("Invalid argument(s) passed to parameter");
            }
            if (expression instanceof QualifiedNameReference) {
                throw new IllegalArgumentException(String.format(
                        "Can't use column reference in property assignment \"%s = %s\". Use literals instead.",
                        key,
                        ((QualifiedNameReference) expression).getName().toString()));
            }

            Symbol v = Field.unwrap(expressionAnalyzer.convert(expression, expressionAnalysisContext));
            if (!v.symbolType().isValueSymbol()) {
                throw new UnsupportedFeatureException("Only literals are allowed as parameter values");
            }
            builder.put(key, StringValueSymbolVisitor.INSTANCE.process(v));
        }
        return builder.build();
    }

    @Override
    public AnalyzedStatement newAnalysis(ParameterContext parameterContext) {
        return new CopyAnalyzedStatement(analysisMetaData.referenceInfos(),
                analysisMetaData.functions(), parameterContext, analysisMetaData.referenceResolver());
    }

    @Override
    protected Void visitTable(Table node, CopyAnalyzedStatement context) {
        context.editableTable(TableIdent.of(node));
        return null;
    }
}
