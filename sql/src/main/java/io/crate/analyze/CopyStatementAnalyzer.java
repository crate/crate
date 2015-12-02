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

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.relations.NameFieldProvider;
import io.crate.analyze.relations.QueriedDocTable;
import io.crate.analyze.symbol.Reference;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.ValueSymbolVisitor;
import io.crate.analyze.where.WhereClauseAnalyzer;
import io.crate.exceptions.PartitionUnknownException;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.metadata.*;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.sql.tree.*;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Singleton
public class CopyStatementAnalyzer {

    private final AnalysisMetaData analysisMetaData;

    @Inject
    public CopyStatementAnalyzer(AnalysisMetaData analysisMetaData) {
        this.analysisMetaData = analysisMetaData;
    }

    public CopyFromAnalyzedStatement convertCopyFrom(CopyFrom node, Analysis analysis) {
        analysis.expectsAffectedRows(true);

        DocTableInfo tableInfo = analysisMetaData.referenceInfos().getWritableTable(
                                TableIdent.of(node.table(), analysis.parameterContext().defaultSchema()));
        DocTableRelation tableRelation = new DocTableRelation(tableInfo);

        String partitionIdent = null;
        if (!node.table().partitionProperties().isEmpty()) {
            partitionIdent = PartitionPropertiesAnalyzer.toPartitionIdent(
                    tableInfo,
                    node.table().partitionProperties(),
                    analysis.parameterContext().parameters());
        }

        Context context = new Context(analysisMetaData, analysis.parameterContext(), tableRelation, true);
        Settings settings = processGenericProperties(node.genericProperties(), context);
        Symbol uri = context.processExpression(node.path());

        return new CopyFromAnalyzedStatement(tableInfo, settings, uri, partitionIdent);
    }

    public CopyToAnalyzedStatement convertCopyTo(CopyTo node, Analysis analysis) {
        analysis.expectsAffectedRows(true);

        TableInfo tableInfo = analysisMetaData.referenceInfos().getTableInfo(
                TableIdent.of(node.table(), analysis.parameterContext().defaultSchema()));
        if (!(tableInfo instanceof DocTableInfo)) {
            throw new UnsupportedOperationException(String.format(
                    "Cannot COPY %s TO. COPY TO only supports user tables", tableInfo.ident()));
        }
        DocTableRelation tableRelation = new DocTableRelation((DocTableInfo) tableInfo);

        Context context = new Context(analysisMetaData, analysis.parameterContext(), tableRelation, false);
        Settings settings = processGenericProperties(node.genericProperties(), context);
        Symbol uri = context.processExpression(node.targetUri());
        WhereClause whereClause = null;
        if (node.whereClause().isPresent()) {
            WhereClauseAnalyzer whereClauseAnalyzer = new WhereClauseAnalyzer(analysisMetaData, tableRelation);
            whereClause = whereClauseAnalyzer.analyze(
                    context.expressionAnalyzer.generateWhereClause(node.whereClause(), context.expressionAnalysisContext));
        }

        List<Symbol> outputs = new ArrayList<>();
        QuerySpec querySpec = new QuerySpec();
        List<String> partitions = ImmutableList.of();
        if (!node.table().partitionProperties().isEmpty()) {
            PartitionName partitionName = PartitionPropertiesAnalyzer.toPartitionName(
                    tableRelation.tableInfo(),
                    node.table().partitionProperties(),
                    analysis.parameterContext().parameters());

            if (!partitionExists(tableRelation.tableInfo(), partitionName)) {
                throw new PartitionUnknownException(tableRelation.tableInfo().ident().fqn(), partitionName.ident());
            }
            partitions = ImmutableList.of(partitionName.asIndexName());
        }

        if (whereClause == null) {
            querySpec.where(new WhereClause(null, null, partitions));
        } else if (whereClause.noMatch()) {
            querySpec.where(whereClause);
        } else {
            if (!whereClause.partitions().isEmpty() && !partitions.isEmpty() && !whereClause.partitions().equals(partitions)) {
                throw new IllegalArgumentException("Given partition ident does not match partition evaluated from where clause");
            }

            querySpec.where(
                    new WhereClause(whereClause.query(), whereClause.docKeys().orNull(),
                            partitions.isEmpty() ? whereClause.partitions() : partitions));
        }

        Map<ColumnIdent, Symbol> overwrites = null;
        boolean columnsDefined = false;
        if (!node.columns().isEmpty()) {
            for (Expression expression : node.columns()) {
                outputs.add(DocReferenceConverter.convertIfPossible(context.processExpression(expression), tableRelation.tableInfo()));
            }
            columnsDefined = true;
        } else {
            Reference sourceRef;
            if (tableRelation.tableInfo().isPartitioned() && partitions.isEmpty()) {
                // table is partitioned, insert partitioned columns into the output
                sourceRef = new Reference(tableRelation.tableInfo().getReferenceInfo(DocSysColumns.DOC));
                overwrites = new HashMap<>();
                for (ReferenceInfo referenceInfo : tableRelation.tableInfo().partitionedByColumns()) {
                    overwrites.put(referenceInfo.ident().columnIdent(), new Reference(referenceInfo));
                }
            } else {
                sourceRef = new Reference(tableRelation.tableInfo().getReferenceInfo(DocSysColumns.RAW));
            }
            outputs = ImmutableList.<Symbol>of(sourceRef);
        }
        querySpec.outputs(outputs);

        QueriedDocTable subRelation = new QueriedDocTable(tableRelation, querySpec);
        return new CopyToAnalyzedStatement(subRelation, settings, uri, node.directoryUri(), columnsDefined, overwrites);
    }

    private Settings processGenericProperties(Optional<GenericProperties> genericProperties, Context context) {
        if (genericProperties.isPresent()) {
            return settingsFromProperties(
                    genericProperties.get(),
                    context.expressionAnalyzer,
                    context.expressionAnalysisContext);
        }
        return ImmutableSettings.EMPTY;
    }

    private boolean partitionExists(DocTableInfo table, @Nullable PartitionName partition) {
        if (table.isPartitioned() && partition != null) {
            for (PartitionName partitionName : table.partitions()) {
                if (partitionName.tableIdent().equals(table.ident())
                    && partitionName.equals(partition)) {
                    return true;
                }
            }
        }
        return false;
    }

    private Settings settingsFromProperties(GenericProperties properties,
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


    private static class Context {

        private final ExpressionAnalyzer expressionAnalyzer;
        private final ExpressionAnalysisContext expressionAnalysisContext;

        public Context(AnalysisMetaData analysisMetaData,
                       ParameterContext parameterContext,
                       DocTableRelation tableRelation,
                       boolean forWrite) {
            expressionAnalysisContext = new ExpressionAnalysisContext();
            expressionAnalyzer = new ExpressionAnalyzer(
                    analysisMetaData,
                    parameterContext,
                    new NameFieldProvider(tableRelation),
                    tableRelation);
            expressionAnalyzer.resolveWritableFields(forWrite);
        }

        public Symbol processExpression(Expression expression) {
            return expressionAnalyzer.normalize(expressionAnalyzer.convert(expression, expressionAnalysisContext));
        }
    }
}
