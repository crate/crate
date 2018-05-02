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
import com.google.common.collect.ImmutableSet;
import io.crate.analyze.copy.NodeFilters;
import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.expressions.ExpressionToObjectVisitor;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.relations.NameFieldProvider;
import io.crate.analyze.where.WhereClauseAnalyzer;
import io.crate.data.Row;
import io.crate.exceptions.PartitionUnknownException;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.execution.dsl.projection.WriterProjection;
import io.crate.expression.eval.EvaluatingNormalizer;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.ValueSymbolVisitor;
import io.crate.expression.symbol.format.SymbolPrinter;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.DocReferences;
import io.crate.metadata.Functions;
import io.crate.metadata.GeneratedReference;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.Schemas;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.settings.SettingsApplier;
import io.crate.metadata.settings.SettingsAppliers;
import io.crate.metadata.settings.StringSetting;
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

    private static final StringSetting COMPRESSION_SETTINGS =
        new StringSetting("compression", ImmutableSet.of("gzip"));

    private static final StringSetting OUTPUT_FORMAT_SETTINGS =
        new StringSetting("format", ImmutableSet.of("json_object", "json_array"));

    private static final ImmutableMap<String, SettingsApplier> SETTINGS_APPLIERS =
        ImmutableMap.<String, SettingsApplier>builder()
            .put(COMPRESSION_SETTINGS.name(), new SettingsAppliers.StringSettingsApplier(COMPRESSION_SETTINGS))
            .put(OUTPUT_FORMAT_SETTINGS.name(), new SettingsAppliers.StringSettingsApplier(OUTPUT_FORMAT_SETTINGS))
            .build();
    private final Schemas schemas;
    private final Functions functions;

    CopyAnalyzer(Schemas schemas, Functions functions) {
        this.schemas = schemas;
        this.functions = functions;
    }

    CopyFromAnalyzedStatement convertCopyFrom(CopyFrom node, Analysis analysis) {
        DocTableInfo tableInfo = schemas.getTableInfo(
            RelationName.of(node.table(), analysis.sessionContext().defaultSchema()), Operation.INSERT);
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
            settings = settingsFromProperties(properties, expressionAnalyzer, expressionAnalysisContext);
        }
        Symbol uri = expressionAnalyzer.convert(node.path(), expressionAnalysisContext);
        uri = normalizer.normalize(uri, analysis.transactionContext());

        if (!(uri.valueType() == DataTypes.STRING ||
              uri.valueType() instanceof CollectionType &&
              ((CollectionType) uri.valueType()).innerType() == DataTypes.STRING)) {
            throw CopyFromAnalyzedStatement.raiseInvalidType(uri.valueType());
        }

        return new CopyFromAnalyzedStatement(tableInfo, settings, uri, partitionIdent, nodeFilters);
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

        TableInfo tableInfo = schemas.getTableInfo(
            RelationName.of(node.table(), analysis.sessionContext().defaultSchema()), Operation.COPY_TO);
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
            node.genericProperties(), analysis.parameterContext(), SETTINGS_APPLIERS).build();

        WriterProjection.CompressionType compressionType =
            settingAsEnum(WriterProjection.CompressionType.class, settings.get(COMPRESSION_SETTINGS.name()));
        WriterProjection.OutputFormat outputFormat =
            settingAsEnum(WriterProjection.OutputFormat.class, settings.get(OUTPUT_FORMAT_SETTINGS.name()));

        if (!columnsDefined && outputFormat == WriterProjection.OutputFormat.JSON_ARRAY) {
            throw new UnsupportedFeatureException("Output format not supported without specifying columns.");
        }

        QuerySpec querySpec = new QuerySpec()
            .outputs(outputs)
            .where(createWhereClause(
                node.whereClause(),
                tableRelation,
                partitions,
                normalizer,
                expressionAnalyzer,
                expressionAnalysisContext,
                analysis.transactionContext())
            );
        QueriedTable<DocTableRelation> subRelation = new QueriedTable<>(tableRelation, querySpec);
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

    private WhereClause createWhereClause(Optional<Expression> where,
                                          DocTableRelation tableRelation,
                                          List<String> partitions,
                                          EvaluatingNormalizer normalizer,
                                          ExpressionAnalyzer expressionAnalyzer,
                                          ExpressionAnalysisContext expressionAnalysisContext,
                                          TransactionContext transactionContext) {
        WhereClause whereClause = null;
        if (where.isPresent()) {
            WhereClauseAnalyzer whereClauseAnalyzer = new WhereClauseAnalyzer(functions, tableRelation);
            Symbol query = expressionAnalyzer.convert(where.get(), expressionAnalysisContext);
            whereClause = whereClauseAnalyzer.analyze(normalizer.normalize(query, transactionContext), transactionContext);
        }

        if (whereClause == null) {
            return new WhereClause(null, null, partitions, Collections.emptySet());
        } else if (whereClause.noMatch()) {
            return whereClause;
        } else {
            if (!whereClause.partitions().isEmpty() && !partitions.isEmpty() &&
                !whereClause.partitions().equals(partitions)) {
                throw new IllegalArgumentException("Given partition ident does not match partition evaluated from where clause");
            }

            return new WhereClause(
                whereClause.query(),
                whereClause.docKeys().orElse(null),
                partitions.isEmpty() ? whereClause.partitions() : partitions,
                whereClause.clusteredBy()
            );
        }
    }

    private static Settings settingsFromProperties(Map<String, Expression> properties,
                                                   ExpressionAnalyzer expressionAnalyzer,
                                                   ExpressionAnalysisContext expressionAnalysisContext) {
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

            Symbol v = expressionAnalyzer.convert(expression, expressionAnalysisContext);
            if (!v.symbolType().isValueSymbol()) {
                throw new UnsupportedFeatureException("Only literals are allowed as parameter values");
            }
            builder.put(key, ValueSymbolVisitor.STRING.process(v));
        }
        return builder.build();
    }
}
