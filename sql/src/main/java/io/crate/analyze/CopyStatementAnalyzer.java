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
import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.relations.NameFieldProvider;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.ValueSymbolVisitor;
import io.crate.exceptions.PartitionUnknownException;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.metadata.PartitionName;
import io.crate.metadata.TableIdent;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.TableInfo;
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
    public CopyAnalyzedStatement visitCopyTo(CopyTo node, Analysis analysis) {
        CopyAnalyzedStatement statement = new CopyAnalyzedStatement();
        statement.mode(CopyAnalyzedStatement.Mode.TO);

        TableInfo tableInfo = analysisMetaData.referenceInfos().getTableInfo(
                TableIdent.of(node.table(), analysis.parameterContext().defaultSchema()));
        if (!(tableInfo instanceof DocTableInfo)) {
            throw new UnsupportedOperationException(String.format(
                    "Cannot COPY %s TO. COPY TO only supports user tables", tableInfo.ident()));
        }
        statement.table((DocTableInfo) tableInfo);
        DocTableRelation tableRelation = new DocTableRelation(statement.table());

        ExpressionAnalyzer expressionAnalyzer = new ExpressionAnalyzer(
                analysisMetaData,
                analysis.parameterContext(),
                new NameFieldProvider(tableRelation),
                tableRelation);
        ExpressionAnalysisContext expressionAnalysisContext = new ExpressionAnalysisContext();

        if (node.genericProperties().isPresent()) {
            statement.settings(settingsFromProperties(
                    node.genericProperties().get(),
                    expressionAnalyzer,
                    expressionAnalysisContext));
        }

        if (!node.table().partitionProperties().isEmpty()) {
            String partitionIdent = PartitionPropertiesAnalyzer.toPartitionIdent(
                    statement.table(),
                    node.table().partitionProperties(),
                    analysis.parameterContext().parameters());

            if (!partitionExists(statement.table(), partitionIdent)) {
                throw new PartitionUnknownException(tableInfo.ident().fqn(), partitionIdent);
            }
            statement.partitionIdent(partitionIdent);
        }
        Symbol uri = expressionAnalyzer.normalize(expressionAnalyzer.convert(node.targetUri(), expressionAnalysisContext));
        statement.uri(uri);
        statement.directoryUri(node.directoryUri());

        List<Symbol> columns = new ArrayList<>(node.columns().size());
        for (Expression expression : node.columns()) {
            columns.add(expressionAnalyzer.normalize(expressionAnalyzer.convert(expression, expressionAnalysisContext)));
        }
        statement.selectedColumns(columns);
        return statement;
    }

    private boolean partitionExists(DocTableInfo table, @Nullable String partitionIdent) {
        if (table.isPartitioned() && partitionIdent != null) {
            for (PartitionName partitionName : table.partitions()) {
                if (partitionName.tableIdent().equals(table.ident())
                    && partitionName.ident().equals(partitionIdent)) {
                    return true;
                }
            }
        }
        return false;
    }

    static Settings settingsFromProperties(GenericProperties properties,
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

            Symbol v = expressionAnalyzer.convert(expression, expressionAnalysisContext);
            if (!v.symbolType().isValueSymbol()) {
                throw new UnsupportedFeatureException("Only literals are allowed as parameter values");
            }
            builder.put(key, ValueSymbolVisitor.STRING.process(v));
        }
        return builder.build();
    }

}
