/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.planner.statement;

import static io.crate.analyze.CopyStatementSettings.COMPRESSION_SETTING;
import static io.crate.analyze.CopyStatementSettings.INPUT_FORMAT_SETTING;
import static io.crate.analyze.CopyStatementSettings.settingAsEnum;
import static io.crate.analyze.GenericPropertiesConverter.genericPropertiesToSettings;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.jetbrains.annotations.Nullable;

import io.crate.execution.dsl.projection.AbstractIndexWriterProjection;
import io.crate.execution.dsl.projection.ColumnIndexWriterProjection;
import io.crate.execution.dsl.projection.ColumnIndexWriterReturnSummaryProjection;
import io.crate.execution.dsl.projection.MergeCountProjection;
import io.crate.execution.dsl.projection.Projection;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.Settings;

import com.carrotsearch.hppc.cursors.ObjectCursor;

import io.crate.analyze.AnalyzedCopyFrom;
import io.crate.analyze.AnalyzedCopyFromReturnSummary;
import io.crate.analyze.BoundCopyFrom;
import io.crate.analyze.CopyFromParserProperties;
import io.crate.analyze.PartitionPropertiesAnalyzer;
import io.crate.analyze.SymbolEvaluator;
import io.crate.analyze.copy.NodeFilters;
import io.crate.common.annotations.VisibleForTesting;
import io.crate.common.collections.Lists2;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.execution.dsl.phases.FileUriCollectPhase;
import io.crate.execution.dsl.phases.NodeOperationTree;
import io.crate.execution.dsl.projection.builder.InputColumns;
import io.crate.execution.engine.JobLauncher;
import io.crate.execution.engine.NodeOperationTreeGenerator;
import io.crate.execution.engine.pipeline.LimitAndOffset;
import io.crate.expression.reference.file.SourceLineNumberExpression;
import io.crate.expression.reference.file.SourceParsingFailureExpression;
import io.crate.expression.reference.file.SourceUriExpression;
import io.crate.expression.reference.file.SourceUriFailureExpression;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.GeneratedReference;
import io.crate.metadata.NodeContext;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.Merge;
import io.crate.planner.Plan;
import io.crate.planner.PlannerContext;
import io.crate.planner.node.dql.Collect;
import io.crate.planner.operators.SubQueryResults;
import io.crate.types.DataTypes;

public final class CopyFromPlan implements Plan {

    private final AnalyzedCopyFrom copyFrom;

    public CopyFromPlan(AnalyzedCopyFrom copyFrom) {
        this.copyFrom = copyFrom;
    }

    public AnalyzedCopyFrom copyFrom() {
        return copyFrom;
    }

    @Override
    public StatementType type() {
        return StatementType.COPY;
    }

    @Override
    public void executeOrFail(DependencyCarrier dependencies,
                              PlannerContext plannerContext,
                              RowConsumer consumer,
                              Row params,
                              SubQueryResults subQueryResults) {

        var boundedCopyFrom = bind(
            copyFrom,
            plannerContext.transactionContext(),
            plannerContext.nodeContext(),
            params,
            subQueryResults);

        ExecutionPlan plan = planCopyFromExecution(
            copyFrom,
            boundedCopyFrom,
            dependencies.clusterService().state().nodes(),
            plannerContext,
            params,
            subQueryResults);

        NodeOperationTree nodeOpTree = NodeOperationTreeGenerator
            .fromPlan(plan, dependencies.localNodeId());

        JobLauncher jobLauncher = dependencies.phasesTaskFactory()
            .create(plannerContext.jobId(), List.of(nodeOpTree));

        jobLauncher.execute(
            consumer,
            plannerContext.transactionContext(),
            boundedCopyFrom.settings().getAsBoolean("wait_for_completion", true));
    }

    @VisibleForTesting
    public static BoundCopyFrom bind(AnalyzedCopyFrom copyFrom,
                                     CoordinatorTxnCtx txnCtx,
                                     NodeContext nodeCtx,
                                     Row parameters,
                                     SubQueryResults subQueryResults) {
        Function<? super Symbol, Object> eval = x -> SymbolEvaluator.evaluate(
            txnCtx,
            nodeCtx,
            x,
            parameters,
            subQueryResults
        );

        String partitionIdent;
        if (!copyFrom.table().partitionProperties().isEmpty()) {
            partitionIdent = PartitionPropertiesAnalyzer
                .toPartitionName(
                    copyFrom.tableInfo(),
                    Lists2.map(copyFrom.table().partitionProperties(), x -> x.map(eval)))
                .ident();
        } else {
            partitionIdent = null;
        }
        var properties = copyFrom.properties().map(eval);
        var nodeFiltersPredicate = discoveryNodePredicate(
            properties.properties().getOrDefault(NodeFilters.NAME, null));
        var settings = genericPropertiesToSettings(properties);
        var inputFormat = settingAsEnum(
            FileUriCollectPhase.InputFormat.class,
            settings.get(INPUT_FORMAT_SETTING.getKey(), INPUT_FORMAT_SETTING.getDefault(Settings.EMPTY)));
        // TODO make FileUriCollectPhase ctor accept an uri of the List<String>
        // instead of the Symbol type, such as the uri can be evaluated and converted
        // to the required type already at this stage, but not later on in FileCollectSource.
        var boundedURI = validateAndConvertToLiteral(eval.apply(copyFrom.uri()));
        var header = settings.getAsBoolean("header", true);
        var targetColumns = copyFrom.targetColumns();
        if (!header && copyFrom.targetColumns().isEmpty()) {
            // JSON behaves like CSV with no-header - we take all table columns.
            targetColumns = Lists2.map(copyFrom.tableInfo().columns(), Reference::toString);
        }

        return new BoundCopyFrom(
            copyFrom.tableInfo(),
            partitionIdent,
            targetColumns,
            settings,
            boundedURI,
            inputFormat,
            nodeFiltersPredicate);
    }

    public static ExecutionPlan planCopyFromExecution(AnalyzedCopyFrom copyFrom,
                                                      BoundCopyFrom boundedCopyFrom,
                                                      DiscoveryNodes allNodes,
                                                      PlannerContext context,
                                                      Row params,
                                                      SubQueryResults subQueryResults) {

        /*
         * Create a plan that reads json-objects-lines from a file
         * and then executes upsert requests to index the data
         */
        DocTableInfo table = boundedCopyFrom.tableInfo();
        String partitionIdent = boundedCopyFrom.partitionIdent();
        List<String> partitionValues = Collections.emptyList();
        if (partitionIdent != null) {
            assert table.isPartitioned() : "table must be partitioned if partitionIdent is set";
            // partitionIdent is present -> possible to index raw source into concrete es index
            partitionValues = PartitionName.decodeIdent(partitionIdent);
        }

        // need to exclude _id columns; they're auto generated and won't be available in the files being imported
        ColumnIdent clusteredBy = table.clusteredBy();
        if (DocSysColumns.ID.equals(clusteredBy)) {
            clusteredBy = null;
        }
        List<Reference> primaryKeyRefs = table.primaryKey().stream()
            .filter(r -> !r.equals(DocSysColumns.ID))
            .map(table::getReference)
            .collect(Collectors.toList());

        // Those columns must be collected regardless of their presence in targetColumns
        // since they are used for calculating shard id.
        // Collecting all top-level table columns might miss those columns if they are sub-columns, hence special collecting.
        Set<Reference> targetColumns = getColumnsRequiredForShardIdCalc(
            primaryKeyRefs,
            table.partitionedByColumns(),
            clusteredBy == null ? null : table.getReference(clusteredBy)
        );

        List<Symbol> toCollect = new ArrayList<>(targetColumns);
        Symbol clusteredByInputCol = null;

        // Add top-level columns (filter if we know already target columns) in addition to required sub-columns added above.
        // If required columns are top-level columns, they are already added and ignored here.
        table.columns()
            .stream()
            .filter(ref ->
                // if targetColumns are empty CSV header/json keys are source of the truth and plan has to be adjusted in runtime to
                // either dynamically add columns or remove table columns, not listed in header.
                boundedCopyFrom.targetColumns().isEmpty()
                    ||
                    // Regular filtering
                    (boundedCopyFrom.targetColumns().isEmpty() == false && boundedCopyFrom.targetColumns().contains(ref.column().sqlFqn()))
            )
            .forEach(ref -> {
                boolean added = targetColumns.add(ref);
                if (added) {
                    toCollect.add(ref);
                }
            });

        List<Reference> targetColsInCorrectOrder = new ArrayList<>(targetColumns);
        Collections.sort(targetColsInCorrectOrder, Comparator.comparingInt(Reference::position));

        List<Reference> targetColsExclPartitionCols = new ArrayList<>();
        for (Reference column : targetColsInCorrectOrder) {
            if (table.partitionedBy().contains(column.column())) {
                continue;
            }
            targetColsExclPartitionCols.add(column);
        }


        InputColumns.SourceSymbols sourceSymbols = new InputColumns.SourceSymbols(toCollect);

        if (clusteredBy != null) {
            clusteredByInputCol = InputColumns.create(table.getReference(clusteredBy), sourceSymbols);
        }

        ColumnIndexWriterProjection indexWriterProjection;
        List<? extends Symbol> projectionOutputs = AbstractIndexWriterProjection.OUTPUTS;
        boolean returnSummary = copyFrom instanceof AnalyzedCopyFromReturnSummary;

        boolean failFast = boundedCopyFrom.settings().getAsBoolean("fail_fast", false);
        boolean overwriteDuplicates = boundedCopyFrom.settings().getAsBoolean("overwrite_duplicates", false);
        boolean validation = false; //temporal change to compare with anoterh branch//boundedCopyFrom.settings().getAsBoolean("validation", true);
        if (returnSummary || failFast) {

            final InputColumn sourceUriSymbol = new InputColumn(toCollect.size(), DataTypes.STRING);
            toCollect.add(SourceUriExpression.getReferenceForRelation(table.ident()));

            final InputColumn sourceUriFailureSymbol = new InputColumn(toCollect.size(), DataTypes.STRING);
            toCollect.add(SourceUriFailureExpression.getReferenceForRelation(table.ident()));

            final InputColumn lineNumberSymbol = new InputColumn(toCollect.size(), DataTypes.LONG);
            toCollect.add(SourceLineNumberExpression.getReferenceForRelation(table.ident()));

            final InputColumn sourceParsingFailureSymbol = new InputColumn(toCollect.size(), DataTypes.STRING);
            toCollect.add(SourceParsingFailureExpression.getReferenceForRelation(table.ident()));

            if (returnSummary) {
                List<? extends Symbol> fields = ((AnalyzedCopyFromReturnSummary) copyFrom).outputs();
                projectionOutputs = InputColumns.create(fields, new InputColumns.SourceSymbols(fields));
            }

            indexWriterProjection = new ColumnIndexWriterReturnSummaryProjection(
                table.ident(),
                partitionIdent,
                table.primaryKey(),
                targetColsInCorrectOrder,
                targetColsExclPartitionCols,
                InputColumns.create(targetColsExclPartitionCols, sourceSymbols),
                false, // Irrelevant for COPY FROM
                overwriteDuplicates,
                failFast,
                validation,
                null, // ON UPDATE SET assignments is irrelevant for COPY FROM
                InputColumns.create(primaryKeyRefs, sourceSymbols),
                InputColumns.create(table.partitionedByColumns(), sourceSymbols),
                table.clusteredBy(),
                clusteredByInputCol,
                boundedCopyFrom.settings(),
                table.isPartitioned(), // autoCreateIndices
                projectionOutputs,
                List.of(), // copyFrom.outputs() is null,
                sourceUriSymbol,
                sourceUriFailureSymbol,
                sourceParsingFailureSymbol,
                lineNumberSymbol
            );
        } else {

            indexWriterProjection = new ColumnIndexWriterProjection(
                table.ident(),
                partitionIdent,
                table.primaryKey(),
                targetColsInCorrectOrder,
                targetColsExclPartitionCols,
                InputColumns.create(targetColsExclPartitionCols, sourceSymbols),
                false, // Irrelevant for COPY FROM
                overwriteDuplicates,
                false, // fail fast is irrelevant for regular COPY FROM
                validation,
                null, // ON UPDATE SET assignments is irrelevant for COPY FROM
                InputColumns.create(primaryKeyRefs, sourceSymbols),
                InputColumns.create(table.partitionedByColumns(), sourceSymbols),
                table.clusteredBy(),
                clusteredByInputCol,
                boundedCopyFrom.settings(),
                table.isPartitioned(), // autoCreateIndices
                projectionOutputs,
                List.of() // copyFrom.outputs() is null
            );
        }

        // We generally don't unwrap generated references as we handle them in the Indexer.
        // However, PARTITIONED BY columns are not included into targetColumns, so if they are generated we need to unwrap them here.
        List<Symbol> unwrappedToCollect = toCollect
            .stream()
            .map(symbol -> {
                if (symbol instanceof GeneratedReference genRef && table.partitionedByColumns().contains(genRef)) {
                    return genRef.generatedExpression();
                } else {
                    return symbol;
                }
            })
            .collect(Collectors.toList());

        // if there are partitionValues (we've had a PARTITION clause in the statement)
        // we need to use the calculated partition values because the partition columns are likely NOT in the data being read
        // the partitionedBy-inputColumns created for the projection are still valid because the positions are not changed
        if (partitionValues != null) {
            rewriteToCollectToUsePartitionValues(table.partitionedByColumns(), partitionValues, unwrappedToCollect);
        }

        FileUriCollectPhase collectPhase = new FileUriCollectPhase(
            context.jobId(),
            context.nextExecutionPhaseId(),
            "copyFrom",
            getExecutionNodes(
                allNodes,
                boundedCopyFrom.settings().getAsInt("num_readers", allNodes.getSize()),
                boundedCopyFrom.nodePredicate()),
            boundedCopyFrom.uri(),
            boundedCopyFrom.targetColumns(),
            unwrappedToCollect,
            Collections.emptyList(),
            COMPRESSION_SETTING.getOrNull(boundedCopyFrom.settings()),
            boundedCopyFrom.settings().getAsBoolean("shared", null),
            CopyFromParserProperties.of(boundedCopyFrom.settings()),
            boundedCopyFrom.inputFormat(),
            boundedCopyFrom.settings()
        );

        Collect collect = new Collect(collectPhase, LimitAndOffset.NO_LIMIT, 0, 1, -1, null);
        // add the projection to the plan to ensure that the outputs are correctly set to the projection outputs
        collect.addProjection(indexWriterProjection);

        List<Projection> handlerProjections;
        if (returnSummary) {
            handlerProjections = Collections.emptyList();
        } else {
            handlerProjections = List.of(MergeCountProjection.INSTANCE);
        }
        return Merge.ensureOnHandler(collect, context, handlerProjections);
    }

    private static void rewriteToCollectToUsePartitionValues(List<Reference> partitionedByColumns,
                                                             List<String> partitionValues,
                                                             List<Symbol> toCollect) {
        for (int i = 0; i < partitionValues.size(); i++) {
            Reference partitionedByColumn = partitionedByColumns.get(i);
            int idx = toCollect.indexOf(partitionedByColumn);

            if (idx > -1) {
                toCollect.set(idx, Literal.of(partitionValues.get(i)));
            }
        }
    }

    /**
     * To generate the upsert request the following is required:
     *
     *  - relationName + partitionIdent / partitionValues
     *      -> to retrieve the indexName
     *
     *  - primaryKeys + clusteredBy  (+ indexName)
     *      -> to calculate the shardId
     */
    private static Set<Reference> getColumnsRequiredForShardIdCalc(List<Reference> primaryKeyRefs,
                                                                   List<Reference> partitionedByRefs,
                                                                   @Nullable Reference clusteredBy) {
        HashSet<Reference> toCollectUnique = new HashSet<>();
        primaryKeyRefs.forEach(r -> toCollectUnique.add(r));
        partitionedByRefs.forEach(r -> toCollectUnique.add(r));
        if (clusteredBy != null) {
            toCollectUnique.add(clusteredBy);
        }
        return toCollectUnique;
    }

    private static Collection<String> getExecutionNodes(DiscoveryNodes allNodes,
                                                        int maxNodes,
                                                        final Predicate<DiscoveryNode> nodeFilters) {
        int counter = maxNodes;
        final List<String> nodes = new ArrayList<>(allNodes.getSize());
        for (ObjectCursor<DiscoveryNode> cursor : allNodes.getDataNodes().values()) {
            if (nodeFilters.test(cursor.value) && counter-- > 0) {
                nodes.add(cursor.value.getId());
            }
        }
        return nodes;
    }

    private static Symbol validateAndConvertToLiteral(Object uri) {
        if (uri instanceof String) {
            return Literal.of(DataTypes.STRING.sanitizeValue(uri));
        } else if (uri instanceof List) {
            Object value = ((List) uri).get(0);
            if (!(value instanceof String)) {
                throw AnalyzedCopyFrom.raiseInvalidType(DataTypes.guessType(uri));
            }
            return Literal.of(DataTypes.STRING_ARRAY, DataTypes.STRING_ARRAY.sanitizeValue(uri));
        }
        throw AnalyzedCopyFrom.raiseInvalidType(DataTypes.guessType(uri));
    }

    private static Predicate<DiscoveryNode> discoveryNodePredicate(@Nullable Object nodeFilter) {
        if (nodeFilter == null) {
            return discoveryNode -> true;
        }
        try {
            return NodeFilters.fromMap((Map) nodeFilter);
        } catch (ClassCastException e) {
            throw new IllegalArgumentException(String.format(
                Locale.ENGLISH,
                "Invalid parameter passed to %s. Expected an object with name or id keys and string values. Got '%s'",
                NodeFilters.NAME, nodeFilter));
        }
    }
}
