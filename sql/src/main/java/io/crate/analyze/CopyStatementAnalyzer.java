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
import io.crate.analyze.relations.NameFieldProvider;
import io.crate.analyze.relations.TableRelation;
import io.crate.exceptions.PartitionUnknownException;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.metadata.PartitionName;
import io.crate.metadata.TableIdent;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.symbol.Symbol;
import io.crate.planner.symbol.SymbolFormatter;
import io.crate.planner.symbol.ValueSymbolVisitor;
import io.crate.sql.tree.*;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Singleton
public class CopyStatementAnalyzer extends DefaultTraversalVisitor<CopyAnalyzedStatement, Analysis> {

    private final AnalysisMetaData analysisMetaData;

    @Inject
    public CopyStatementAnalyzer(AnalysisMetaData analysisMetaData) {
        this.analysisMetaData = analysisMetaData;
    }

    public AnalyzedStatement analyze(Node node, Analysis analysis) {
        analysis.expectsAffectedRows(true);
        return super.process(node, analysis);

    }

    @Override
    public CopyAnalyzedStatement visitCopyFromStatement(CopyFromStatement node, Analysis analysis) {
        CopyAnalyzedStatement statement = new CopyAnalyzedStatement();
        TableInfo tableInfo = analysisMetaData.referenceInfos().getWritableTable(
                TableIdent.of(node.table(), analysis.parameterContext().defaultSchema()));
        statement.table(tableInfo);
        TableRelation tableRelation = new TableRelation(tableInfo);

        ExpressionAnalyzer expressionAnalyzer = new ExpressionAnalyzer(
                analysisMetaData,
                analysis.parameterContext(),
                new NameFieldProvider(tableRelation));
        expressionAnalyzer.resolveWritableFields(true);
        ExpressionAnalysisContext expressionAnalysisContext = new ExpressionAnalysisContext();

        Symbol pathSymbol = tableRelation.resolve(expressionAnalyzer.convert(node.path(), expressionAnalysisContext));
        statement.uri(pathSymbol);
        if (tableInfo.schemaInfo().systemSchema() || (tableInfo.isAlias() && !tableInfo.isPartitioned())) {
            throw new UnsupportedOperationException(
                    String.format("Cannot COPY FROM %s INTO '%s', table is read-only", SymbolFormatter.format(pathSymbol), tableInfo));
        }
        if (node.genericProperties().isPresent()) {
            statement.settings(settingsFromProperties(
                    node.genericProperties().get(),
                    tableRelation,
                    expressionAnalyzer,
                    expressionAnalysisContext));
        }
        statement.mode(CopyAnalyzedStatement.Mode.FROM);

        if (!node.table().partitionProperties().isEmpty()) {
            statement.partitionIdent(PartitionPropertiesAnalyzer.toPartitionIdent(
                    tableInfo,
                    node.table().partitionProperties(),
                    analysis.parameterContext().parameters()));
        }

        return statement;
    }

    @Override
    public CopyAnalyzedStatement visitCopyTo(CopyTo node, Analysis analysis) {
        CopyAnalyzedStatement statement = new CopyAnalyzedStatement();
        statement.mode(CopyAnalyzedStatement.Mode.TO);
        TableInfo tableInfo = analysisMetaData.referenceInfos().getTableInfo(
                TableIdent.of(node.table(), analysis.parameterContext().defaultSchema()));
        statement.table(tableInfo);
        TableRelation tableRelation = new TableRelation(tableInfo);

        ExpressionAnalyzer expressionAnalyzer = new ExpressionAnalyzer(
                analysisMetaData,
                analysis.parameterContext(),
                new NameFieldProvider(tableRelation));
        ExpressionAnalysisContext expressionAnalysisContext = new ExpressionAnalysisContext();

        if (node.genericProperties().isPresent()) {
            statement.settings(settingsFromProperties(
                    node.genericProperties().get(),
                    tableRelation,
                    expressionAnalyzer,
                    expressionAnalysisContext));
        }

        if (!node.table().partitionProperties().isEmpty()) {
            String partitionIdent = PartitionPropertiesAnalyzer.toPartitionIdent(
                    tableInfo,
                    node.table().partitionProperties(),
                    analysis.parameterContext().parameters());

            if (!partitionExists(tableInfo, partitionIdent)){
                throw new PartitionUnknownException(tableInfo.ident().fqn(), partitionIdent);
            }
            statement.partitionIdent(partitionIdent);
        }
        Symbol uri = expressionAnalyzer.convert(node.targetUri(), expressionAnalysisContext);
        statement.uri(tableRelation.resolve(uri));
        statement.directoryUri(node.directoryUri());

        List<Symbol> columns = new ArrayList<>(node.columns().size());
        for (Expression expression : node.columns()) {
            columns.add(tableRelation.resolve(expressionAnalyzer.convert(expression, expressionAnalysisContext)));
        }
        statement.selectedColumns(columns);
        return statement;
    }

    private boolean partitionExists(TableInfo table, @Nullable String partitionIdent) {
        if (table.isPartitioned() && partitionIdent != null) {
            return table.partitions().contains(PartitionName.fromPartitionIdent(table.ident().schema(), table.ident().name(), partitionIdent));
        }
        return false;
    }

    private Settings settingsFromProperties(GenericProperties properties,
                                            TableRelation tableRelation,
                                            ExpressionAnalyzer expressionAnalyzer,
                                            ExpressionAnalysisContext expressionAnalysisContext) {
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
            builder.put(key, ValueSymbolVisitor.STRING.process(v));
        }
        return builder.build();
    }

}
