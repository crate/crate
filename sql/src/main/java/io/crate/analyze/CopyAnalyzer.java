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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.crate.analyze.copy.NodeFilters;
import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.expressions.ExpressionToObjectVisitor;
import io.crate.analyze.expressions.ExpressionToStringVisitor;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.relations.NameFieldProvider;
import io.crate.data.Row;
import io.crate.exceptions.PartitionUnknownException;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.execution.dsl.phases.FileUriCollectPhase;
import io.crate.execution.dsl.projection.WriterProjection;
import io.crate.expression.eval.EvaluatingNormalizer;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.format.SymbolPrinter;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.DocReferences;
import io.crate.metadata.Functions;
import io.crate.metadata.GeneratedReference;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Reference;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.Schemas;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.settings.Validators;
import io.crate.metadata.table.Operation;
import io.crate.metadata.table.TableInfo;
import io.crate.sql.tree.ArrayLiteral;
import io.crate.sql.tree.Assignment;
import io.crate.sql.tree.CopyFrom;
import io.crate.sql.tree.CopyTo;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.QualifiedNameReference;
import io.crate.types.CollectionType;
import io.crate.types.DataTypes;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
class CopyAnalyzer {

    private static final Setting<String> COMPRESSION_SETTING =
        Setting.simpleString("compression", Validators.stringValidator("compression","gzip"), Setting.Property.Dynamic);

    private static final Setting<String> OUTPUT_FORMAT_SETTING =
        Setting.simpleString("format", Validators.stringValidator("format","json_object", "json_array"), Setting.Property.Dynamic);

    private static final Setting<String> INPUT_FORMAT_SETTING =
        new Setting<>("format", "json", (s) -> s, Validators.stringValidator("format","json", "csv"), Setting.Property.Dynamic);

    private static final ImmutableMap<String, Setting> OUTPUT_SETTINGS = ImmutableMap.<String, Setting>builder()
        .put(COMPRESSION_SETTING.getKey(), COMPRESSION_SETTING)
        .put(OUTPUT_FORMAT_SETTING.getKey(), OUTPUT_FORMAT_SETTING)
        .build();

    private final Schemas schemas;
    private final Functions functions;

    CopyAnalyzer(Schemas schemas, Functions functions) {
        this.schemas = schemas;
        this.functions = functions;
    }

    CopyFromAnalyzedStatement convertCopyFrom(CopyFrom node, Analysis analysis) {
        DocTableInfo tableInfo = (DocTableInfo)
            schemas.resolveTableInfo(node.table().getName(), Operation.INSERT, analysis.sessionContext().searchPath());
        DocTableRelation tableRelation = new DocTableRelation(tableInfo);

        String partitionIdent = null;
        if (!node.table().partitionProperties().isEmpty()) {
            partitionIdent = PartitionPropertiesAnalyzer.toPartitionIdent(
                tableInfo,
                node.table().partitionProperties(),
                analysis.parameterContext().parameters());
        }

        EvaluatingNormalizer normalizer = new EvaluatingNormalizer(
            functions,
            RowGranularity.CLUSTER,
            null,
            tableRelation);
        ExpressionAnalyzer expressionAnalyzer = createExpressionAnalyzer(analysis, tableRelation, Operation.INSERT);
        ExpressionAnalysisContext expressionAnalysisContext = new ExpressionAnalysisContext();
        Predicate<DiscoveryNode> nodeFilters = discoveryNode -> true;
        Settings settings = Settings.EMPTY;
        if (!node.genericProperties().isEmpty()) {
            // copy map as items are removed. The GenericProperties map is cached in the query cache and removing
            // items would cause subsequent queries that hit the cache to have different genericProperties
            Map<String, Expression> properties = new HashMap<>(node.genericProperties().properties());
            nodeFilters = discoveryNodePredicate(analysis.parameterContext().parameters(), properties.remove(NodeFilters.NAME));
            settings = settingsFromProperties(properties);
        }
        Symbol uri = expressionAnalyzer.convert(node.path(), expressionAnalysisContext);
        uri = normalizer.normalize(uri, analysis.transactionContext());

        if (!(uri.valueType() == DataTypes.STRING ||
              uri.valueType() instanceof CollectionType &&
              ((CollectionType) uri.valueType()).innerType() == DataTypes.STRING)) {
            throw CopyFromAnalyzedStatement.raiseInvalidType(uri.valueType());
        }

        FileUriCollectPhase.InputFormat inputFormat =
            settingAsEnum(FileUriCollectPhase.InputFormat.class, settings.get(INPUT_FORMAT_SETTING.getKey(), INPUT_FORMAT_SETTING.getDefault(Settings.EMPTY)));

        if (node.isReturnSummary()) {
            return new CopyFromReturnSummaryAnalyzedStatement(tableInfo, settings, uri, partitionIdent, nodeFilters, inputFormat);
        }

        return new CopyFromAnalyzedStatement(tableInfo, settings, uri, partitionIdent, nodeFilters, inputFormat);
    }

    private ExpressionAnalyzer createExpressionAnalyzer(Analysis analysis, DocTableRelation tableRelation, Operation operation) {
        return new ExpressionAnalyzer(
            functions,
            analysis.transactionContext(),
            analysis.parameterContext(),
            new NameFieldProvider(tableRelation),
            null,
            operation);
    }

    private static Predicate<DiscoveryNode> discoveryNodePredicate(Row parameters, @Nullable Expression nodeFiltersExpression) {
        if (nodeFiltersExpression == null) {
            return discoveryNode -> true;
        }
        Object nodeFiltersObj = ExpressionToObjectVisitor.convert(nodeFiltersExpression, parameters);
        try {
            return NodeFilters.fromMap((Map) nodeFiltersObj);
        } catch (ClassCastException e) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                "Invalid parameter passed to %s. Expected an object with name or id keys and string values. Got '%s'",
                NodeFilters.NAME, nodeFiltersObj));
        }
    }

    CopyToAnalyzedStatement convertCopyTo(CopyTo node, Analysis analysis) {
        if (!node.directoryUri()) {
            throw new UnsupportedOperationException("Using COPY TO without specifying a DIRECTORY is not supported");
        }

        TableInfo tableInfo =
            schemas.resolveTableInfo(node.table().getName(), Operation.COPY_TO, analysis.sessionContext().searchPath());
        Operation.blockedRaiseException(tableInfo, Operation.READ);
        DocTableRelation tableRelation = new DocTableRelation((DocTableInfo) tableInfo);

        EvaluatingNormalizer normalizer = new EvaluatingNormalizer(
            functions,
            RowGranularity.CLUSTER,
            null,
            tableRelation);
        ExpressionAnalyzer expressionAnalyzer = createExpressionAnalyzer(analysis, tableRelation, Operation.READ);
        ExpressionAnalysisContext expressionAnalysisContext = new ExpressionAnalysisContext();

        Symbol uri = expressionAnalyzer.convert(node.targetUri(), expressionAnalysisContext);
        uri = normalizer.normalize(uri, analysis.transactionContext());
        List<String> partitions = resolvePartitions(
            node.table().partitionProperties(), analysis.parameterContext().parameters(), tableRelation.tableInfo());

        List<Symbol> outputs = new ArrayList<>();
        Map<ColumnIdent, Symbol> overwrites = null;
        boolean columnsDefined = false;
        List<String> outputNames = null;
        if (!node.columns().isEmpty()) {
            outputNames = new ArrayList<>(node.columns().size());
            for (Expression expression : node.columns()) {
                Symbol symbol = expressionAnalyzer.convert(expression, expressionAnalysisContext);
                symbol = normalizer.normalize(symbol, analysis.transactionContext());
                outputNames.add(SymbolPrinter.INSTANCE.printUnqualified(symbol));
                outputs.add(DocReferences.toSourceLookup(symbol));
            }
            columnsDefined = true;
        } else {
            Reference sourceRef;
            if (tableRelation.tableInfo().isPartitioned() && partitions.isEmpty()) {
                // table is partitioned, insert partitioned columns into the output
                overwrites = new HashMap<>();
                for (Reference reference : tableRelation.tableInfo().partitionedByColumns()) {
                    if (!(reference instanceof GeneratedReference)) {
                        overwrites.put(reference.column(), reference);
                    }
                }
                if (overwrites.size() > 0) {
                    sourceRef = tableRelation.tableInfo().getReference(DocSysColumns.DOC);
                } else {
                    sourceRef = tableRelation.tableInfo().getReference(DocSysColumns.RAW);
                }
            } else {
                sourceRef = tableRelation.tableInfo().getReference(DocSysColumns.RAW);
            }
            outputs = ImmutableList.of(sourceRef);
        }

        Settings settings = GenericPropertiesConverter.settingsFromProperties(
            node.genericProperties(), analysis.parameterContext().parameters(), OUTPUT_SETTINGS).build();

        WriterProjection.CompressionType compressionType =
            settingAsEnum(WriterProjection.CompressionType.class, COMPRESSION_SETTING.get(settings));
        WriterProjection.OutputFormat outputFormat =
            settingAsEnum(WriterProjection.OutputFormat.class, OUTPUT_FORMAT_SETTING.get(settings));

        if (!columnsDefined && outputFormat == WriterProjection.OutputFormat.JSON_ARRAY) {
            throw new UnsupportedFeatureException("Output format not supported without specifying columns.");
        }

        QuerySpec querySpec = new QuerySpec()
            .outputs(outputs)
            .where(createWhereClause(
                node.whereClause(),
                partitions,
                normalizer,
                expressionAnalyzer,
                expressionAnalysisContext,
                analysis.transactionContext())
            );
        QueriedTable<DocTableRelation> subRelation = new QueriedTable<>(false, tableRelation, querySpec);
        return new CopyToAnalyzedStatement(
            subRelation, settings, uri, compressionType, outputFormat, outputNames, columnsDefined, overwrites);
    }

    private static <E extends Enum<E>> E settingAsEnum(Class<E> settingsEnum, String settingValue) {
        if (settingValue == null || settingValue.isEmpty()) {
            return null;
        }
        return Enum.valueOf(settingsEnum, settingValue.toUpperCase(Locale.ENGLISH));
    }

    private static List<String> resolvePartitions(List<Assignment> partitionProperties, Row parameters, DocTableInfo table) {
        if (partitionProperties.isEmpty()) {
            return Collections.emptyList();
        }
        PartitionName partitionName = PartitionPropertiesAnalyzer.toPartitionName(
            table,
            partitionProperties,
            parameters);
        if (!table.partitions().contains(partitionName)) {
            throw new PartitionUnknownException(partitionName);
        }
        return Collections.singletonList(partitionName.asIndexName());
    }

    private static WhereClause createWhereClause(Optional<Expression> where,
                                                 List<String> partitions,
                                                 EvaluatingNormalizer normalizer,
                                                 ExpressionAnalyzer expressionAnalyzer,
                                                 ExpressionAnalysisContext expressionAnalysisContext,
                                                 TransactionContext transactionContext) {
        // primary key optimization and partition selection from query happens later
        // based on the queriedRelation in the LogicalPlanner
        if (where.isPresent()) {
            Symbol query = normalizer.normalize(
                expressionAnalyzer.convert(where.get(), expressionAnalysisContext), transactionContext);
            return new WhereClause(query, partitions, Collections.emptySet());
        } else {
            return new WhereClause(null, partitions, Collections.emptySet());
        }
    }

    private static Settings settingsFromProperties(Map<String, Expression> properties) {
        Settings.Builder builder = Settings.builder();
        for (Map.Entry<String, Expression> entry : properties.entrySet()) {
            String key = entry.getKey();
            Expression expression = entry.getValue();
            if (expression instanceof ArrayLiteral) {
                throw new IllegalArgumentException("Invalid argument(s) passed to parameter");
            }
            if (expression instanceof QualifiedNameReference) {
                throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                    "Can't use column reference in property assignment \"%s = %s\". Use literals instead.",
                    key,
                    ((QualifiedNameReference) expression).getName().toString()));
            }
            String value;
            try {
                value = ExpressionToStringVisitor.convert(expression, Row.EMPTY);
            } catch (UnsupportedOperationException e) {
                throw new UnsupportedFeatureException("Only literals are allowed as parameter values. Got " + expression);
            }
            builder.put(key, value);
        }
        return builder.build();
    }
}
