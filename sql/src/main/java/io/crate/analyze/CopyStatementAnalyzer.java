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

import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.relations.NameFieldResolver;
import io.crate.analyze.relations.TableRelation;
import io.crate.exceptions.PartitionUnknownException;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.metadata.PartitionName;
import io.crate.metadata.TableIdent;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.symbol.StringValueSymbolVisitor;
import io.crate.planner.symbol.Symbol;
import io.crate.planner.symbol.SymbolFormatter;
import io.crate.sql.tree.*;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;

import javax.annotation.Nullable;
import java.util.ArrayList;
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
        TableInfo tableInfo = analysisMetaData.referenceInfos().getTableInfoUnsafe(TableIdent.of(node.table()));
        context.table(tableInfo);
        TableRelation tableRelation = new TableRelation(tableInfo);
        setExpressionAnalyzer(tableRelation, context);
        Symbol pathSymbol = tableRelation.resolve(expressionAnalyzer.convert(node.path(), expressionAnalysisContext));
        context.uri(pathSymbol);
        if (tableInfo.schemaInfo().systemSchema() || (tableInfo.isAlias() && !tableInfo.isPartitioned())) {
            throw new UnsupportedOperationException(
                    String.format("Cannot COPY FROM %s INTO '%s', table is read-only", SymbolFormatter.format(pathSymbol), tableInfo));
        }
        if (node.genericProperties().isPresent()) {
            context.settings(settingsFromProperties(node.genericProperties().get(), tableRelation));
        }
        context.mode(CopyAnalyzedStatement.Mode.FROM);

        if (!node.table().partitionProperties().isEmpty()) {
            context.partitionIdent(PartitionPropertiesAnalyzer.toPartitionIdent(
                            tableInfo,
                            node.table().partitionProperties(),
                            context.parameters()));
        }

        return null;
    }

    @Override
    public Void visitCopyTo(CopyTo node, CopyAnalyzedStatement context) {
        context.mode(CopyAnalyzedStatement.Mode.TO);
        TableInfo tableInfo = analysisMetaData.referenceInfos().getTableInfoUnsafe(TableIdent.of(node.table()));
        context.table(tableInfo);
        TableRelation tableRelation = new TableRelation(tableInfo);
        setExpressionAnalyzer(tableRelation, context);
        if (node.genericProperties().isPresent()) {
            context.settings(settingsFromProperties(node.genericProperties().get(), tableRelation));
        }

        if (!node.table().partitionProperties().isEmpty()) {
            String partitionIdent = PartitionPropertiesAnalyzer.toPartitionIdent(
                    tableInfo,
                    node.table().partitionProperties(),
                    context.parameters());

            if (!partitionExists(tableInfo, partitionIdent)){
                throw new PartitionUnknownException(tableInfo.ident().fqn(), partitionIdent);
            }
            context.partitionIdent(partitionIdent);
        }
        Symbol uri = expressionAnalyzer.convert(node.targetUri(), expressionAnalysisContext);
        context.uri(tableRelation.resolve(uri));
        context.directoryUri(node.directoryUri());

        List<Symbol> columns = new ArrayList<>(node.columns().size());
        for (Expression expression : node.columns()) {
            columns.add(tableRelation.resolve(expressionAnalyzer.convert(expression, expressionAnalysisContext)));
        }
        context.selectedColumns(columns);
        return null;
    }

    private boolean partitionExists(TableInfo table, @Nullable String partitionIdent) {
        if (table.isPartitioned() && partitionIdent != null) {
            return table.partitions().contains(PartitionName.fromPartitionIdent(table.ident().schema(), table.ident().name(), partitionIdent));
        }
        return false;
    }

    private void setExpressionAnalyzer(TableRelation tableRelation, CopyAnalyzedStatement context) {
        expressionAnalyzer = new ExpressionAnalyzer(
                analysisMetaData,
                context.parameterContext(),
                new NameFieldResolver(tableRelation));
        if (context.mode() == CopyAnalyzedStatement.Mode.FROM) {
            expressionAnalyzer.resolveWritableFields(true);
        }
        expressionAnalysisContext = new ExpressionAnalysisContext();
    }

    private Settings settingsFromProperties(GenericProperties properties, TableRelation tableRelation) {
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

            Symbol v = tableRelation.resolve(expressionAnalyzer.convert(expression, expressionAnalysisContext));
            if (!v.symbolType().isValueSymbol()) {
                throw new UnsupportedFeatureException("Only literals are allowed as parameter values");
            }
            builder.put(key, StringValueSymbolVisitor.INSTANCE.process(v));
        }
        return builder.build();
    }

    @Override
    public AnalyzedStatement newAnalysis(ParameterContext parameterContext) {
        return new CopyAnalyzedStatement(parameterContext);
    }
}
