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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.relations.NameFieldProvider;
import io.crate.analyze.relations.QueriedDocTable;
import io.crate.analyze.symbol.Reference;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.SymbolFormatter;
import io.crate.analyze.symbol.ValueSymbolVisitor;
import io.crate.analyze.where.WhereClauseAnalyzer;
import io.crate.exceptions.PartitionUnknownException;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.metadata.*;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.settings.StringSetting;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.projection.WriterProjection;
import io.crate.sql.tree.*;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;

import javax.annotation.Nullable;
import java.util.*;

@Singleton
public class CopyStatementAnalyzer {

    private final AnalysisMetaData analysisMetaData;

    private static final String COMPRESSION_SETTING = "compression";
    private static final String OUTPUT_FORMAT_SETTING = "format";

    private static final StringSetting COMPRESSION_SETTINGS = new StringSetting(ImmutableSet.of(
            "gzip"
    )) {
        @Override
        public String name() {
            return COMPRESSION_SETTING;
        }

        @Override
        public boolean isRuntime() {
            return true;
        }
    };

    private static final StringSetting OUTPUT_FORMAT_SETTINGS = new StringSetting(ImmutableSet.of(
            "json_object",
            "json_array"
    )) {
        @Override
        public String name() {
            return OUTPUT_FORMAT_SETTING;
        }

        @Override
        public boolean isRuntime() {
            return true;
        }
    };

    private static final ImmutableMap<String, SettingsApplier> SETTINGS_APPLIERS =
            ImmutableMap.<String, SettingsApplier>builder()
                    .put(COMPRESSION_SETTING, new SettingsAppliers.StringSettingsApplier(COMPRESSION_SETTINGS))
                    .put(OUTPUT_FORMAT_SETTING, new SettingsAppliers.StringSettingsApplier(OUTPUT_FORMAT_SETTINGS))
                    .build();

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
        Settings settings = processCopyToProperties(node.genericProperties(), analysis.parameterContext());

        WriterProjection.CompressionType compressionType = settingAsEnum(WriterProjection.CompressionType.class, settings.get(COMPRESSION_SETTING));
        WriterProjection.OutputFormat outputFormat = settingAsEnum(WriterProjection.OutputFormat.class, settings.get(OUTPUT_FORMAT_SETTING));

        Symbol uri = context.processExpression(node.targetUri());
        List<String> partitions = resolvePartitions(node, analysis, tableRelation);

        List<Symbol> outputs = new ArrayList<>();
        QuerySpec querySpec = new QuerySpec();

        WhereClause whereClause = createWhereClause(node, tableRelation, context, partitions);
        querySpec.where(whereClause);

        Map<ColumnIdent, Symbol> overwrites = null;
        boolean columnsDefined = false;
        List<String> outputNames = null;

        if (!node.columns().isEmpty()) {
            outputNames = new ArrayList<>(node.columns().size());
            for (Expression expression : node.columns()) {
                Symbol symbol = context.processExpression(expression);
                outputNames.add(SymbolFormatter.INSTANCE.formatSimple(symbol));
                outputs.add(DocReferenceConverter.convertIfPossible(symbol, tableRelation.tableInfo()));
            }
            columnsDefined = true;
        } else {
            Reference sourceRef;
            if (tableRelation.tableInfo().isPartitioned() && partitions.isEmpty()) {
                // table is partitioned, insert partitioned columns into the output
                sourceRef = new Reference(tableRelation.tableInfo().getReferenceInfo(DocSysColumns.DOC));
                overwrites = new HashMap<>();
                for (ReferenceInfo referenceInfo : tableRelation.tableInfo().partitionedByColumns()) {
                    if (!(referenceInfo instanceof GeneratedReferenceInfo)) {
                        overwrites.put(referenceInfo.ident().columnIdent(), new Reference(referenceInfo));
                    }
                }
            } else {
                sourceRef = new Reference(tableRelation.tableInfo().getReferenceInfo(DocSysColumns.RAW));
            }
            outputs = ImmutableList.<Symbol>of(sourceRef);
        }
        querySpec.outputs(outputs);

        if (!columnsDefined && outputFormat == WriterProjection.OutputFormat.JSON_ARRAY) {
            throw new UnsupportedFeatureException("Output format not supported without specifying columns.");
        }

        QueriedDocTable subRelation = new QueriedDocTable(tableRelation, querySpec);
        return new CopyToAnalyzedStatement(subRelation, settings, uri, node.directoryUri(), compressionType, outputFormat, outputNames, columnsDefined, overwrites);
    }

    private static <E extends Enum<E>> E settingAsEnum(Class<E> settingsEnum, String settingValue) {
        E setting = null;
        if (settingValue != null) {
            setting = Enum.valueOf(settingsEnum, settingValue.toUpperCase(Locale.ENGLISH));
        }
        return setting;
    }

    private List<String> resolvePartitions(CopyTo node, Analysis analysis, DocTableRelation tableRelation) {
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
        return partitions;
    }

    private WhereClause createWhereClause(CopyTo node, DocTableRelation tableRelation, Context context, List<String> partitions) {
        WhereClause whereClause = null;
        if (node.whereClause().isPresent()) {
            WhereClauseAnalyzer whereClauseAnalyzer = new WhereClauseAnalyzer(analysisMetaData, tableRelation);
            whereClause = whereClauseAnalyzer.analyze(
                    context.expressionAnalyzer.generateWhereClause(node.whereClause(), context.expressionAnalysisContext));
        }

        if (whereClause == null) {
            return new WhereClause(null, null, partitions);
        } else if (whereClause.noMatch()) {
            return whereClause;
        } else {
            if (!whereClause.partitions().isEmpty() && !partitions.isEmpty() && !whereClause.partitions().equals(partitions)) {
                throw new IllegalArgumentException("Given partition ident does not match partition evaluated from where clause");
            }

            return new WhereClause(whereClause.query(), whereClause.docKeys().orNull(),
                            partitions.isEmpty() ? whereClause.partitions() : partitions);
        }
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

    private Settings processCopyToProperties(Optional<GenericProperties> genericProperties, ParameterContext parameterContext) {
        ImmutableSettings.Builder settingsBuilder = ImmutableSettings.builder();

        if (genericProperties.isPresent()) {
            for (Map.Entry<String, Expression> setting : genericProperties.get().properties().entrySet()) {
                SettingsApplier settingsApplier = SETTINGS_APPLIERS.get(setting.getKey());
                if (settingsApplier == null) {
                    throw new IllegalArgumentException(String.format("Unknown setting '%s'", setting.getKey()));
                }

                settingsApplier.apply(settingsBuilder, parameterContext.parameters(), setting.getValue());
            }
        }

        return settingsBuilder.build();
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
