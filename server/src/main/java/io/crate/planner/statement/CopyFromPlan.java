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
import static io.crate.analyze.CopyStatementSettings.NUM_READERS_SETTING;
import static io.crate.analyze.CopyStatementSettings.SHARED_SETTING;
import static io.crate.analyze.CopyStatementSettings.WAIT_FOR_COMPLETION_SETTING;
import static io.crate.analyze.CopyStatementSettings.settingAsEnum;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.Settings;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

import com.carrotsearch.hppc.cursors.ObjectCursor;

import io.crate.analyze.AnalyzedCopyFrom;
import io.crate.analyze.AnalyzedCopyFromReturnSummary;
import io.crate.analyze.BoundCopyFrom;
import io.crate.analyze.CopyFromParserProperties;
import io.crate.analyze.CopyStatementSettings;
import io.crate.analyze.SymbolEvaluator;
import io.crate.analyze.copy.NodeFilters;
import io.crate.common.collections.Lists;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.execution.dsl.phases.FileUriCollectPhase;
import io.crate.execution.dsl.phases.NodeOperationTree;
import io.crate.execution.dsl.projection.AbstractIndexWriterProjection;
import io.crate.execution.dsl.projection.MergeCountProjection;
import io.crate.execution.dsl.projection.Projection;
import io.crate.execution.dsl.projection.SourceIndexWriterProjection;
import io.crate.execution.dsl.projection.SourceIndexWriterReturnSummaryProjection;
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
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.doc.SysColumns;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.Merge;
import io.crate.planner.Plan;
import io.crate.planner.PlannerContext;
import io.crate.planner.node.dql.Collect;
import io.crate.planner.operators.SubQueryResults;
import io.crate.sql.tree.GenericProperties;
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
            plannerContext);

        NodeOperationTree nodeOpTree = NodeOperationTreeGenerator
            .fromPlan(plan, dependencies.localNodeId());

        JobLauncher jobLauncher = dependencies.phasesTaskFactory()
            .create(plannerContext.jobId(), List.of(nodeOpTree));

        jobLauncher.execute(
            consumer,
            plannerContext.transactionContext(),
            WAIT_FOR_COMPLETION_SETTING.get(boundedCopyFrom.settings())
        );
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

        PartitionName partitionName = copyFrom.table().partitionProperties().isEmpty()
            ? null
            : PartitionName.ofAssignmentsUnsafe(copyFrom.tableInfo(), Lists.map(copyFrom.table().partitionProperties(), x -> x.map(eval)));
        String partitionIdent = partitionName == null ? null : partitionName.ident();
        final var properties = copyFrom.properties().map(eval);
        final var nodeFiltersPredicate = discoveryNodePredicate(properties.get(NodeFilters.NAME, null));
        final var settings = Settings.builder().put(properties).build();

        boolean returnSummary = copyFrom instanceof AnalyzedCopyFromReturnSummary;
        boolean waitForCompletion = WAIT_FOR_COMPLETION_SETTING.get(settings);
        if (!waitForCompletion && returnSummary) {
            throw new UnsupportedOperationException(
                "Cannot use RETURN SUMMARY with wait_for_completion=false. Either set wait_for_completion=true, or remove RETURN SUMMARY");
        }
        var inputFormat = settingAsEnum(
            FileUriCollectPhase.InputFormat.class,
            settings.get(INPUT_FORMAT_SETTING.getKey(), INPUT_FORMAT_SETTING.getDefault(Settings.EMPTY)));
        // TODO make FileUriCollectPhase ctor accept an uri of the List<String>
        // instead of the Symbol type, such as the uri can be evaluated and converted
        // to the required type already at this stage, but not later on in FileCollectSource.
        var boundedURI = validateAndConvertToLiteral(eval.apply(copyFrom.uri()), properties);
        var header = settings.getAsBoolean("header", true);
        var targetColumns = copyFrom.targetColumns();
        if (!header && copyFrom.targetColumns().isEmpty()) {
            targetColumns = Lists.map(copyFrom.tableInfo().rootColumns(), Reference::toString);
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
                                                      PlannerContext context) {

        /*
         * Create a plan that reads json-objects-lines from a file
         * and then executes upsert requests to index the data
         */
        DocTableInfo table = boundedCopyFrom.tableInfo();
        String partitionIdent = boundedCopyFrom.partitionIdent();
        List<String> partitionedByNames = Collections.emptyList();
        List<String> partitionValues = Collections.emptyList();
        if (partitionIdent == null) {
            if (table.isPartitioned()) {
                partitionedByNames = Lists.map(table.partitionedBy(), ColumnIdent::fqn);
            }
        } else {
            assert table.isPartitioned() : "table must be partitioned if partitionIdent is set";
            // partitionIdent is present -> possible to index raw source into concrete es index
            partitionValues = PartitionName.decodeIdent(partitionIdent);
        }

        // need to exclude _id columns; they're auto generated and won't be available in the files being imported
        ColumnIdent clusteredBy = table.clusteredBy();
        if (SysColumns.ID.COLUMN.equals(clusteredBy)) {
            clusteredBy = null;
        }
        List<Reference> primaryKeyRefs = table.primaryKey().stream()
            .filter(r -> !r.equals(SysColumns.ID.COLUMN))
            .map(table::getReference)
            .toList();

        List<Symbol> toCollect = getSymbolsRequiredForShardIdCalc(
            primaryKeyRefs,
            table.partitionedByColumns(),
            clusteredBy == null ? null : table.getReference(clusteredBy)
        );
        Reference rawOrDoc = rawOrDoc(table, partitionIdent);
        final int rawOrDocIdx = toCollect.size();
        toCollect.add(rawOrDoc);

        String[] excludes = !partitionedByNames.isEmpty()
            ? partitionedByNames.toArray(new String[0]) : null;

        InputColumns.SourceSymbols sourceSymbols = new InputColumns.SourceSymbols(toCollect);
        Symbol clusteredByInputCol = null;
        if (clusteredBy != null) {
            clusteredByInputCol = InputColumns.create(table.getReference(clusteredBy), sourceSymbols);
        }

        SourceIndexWriterProjection sourceIndexWriterProjection;
        List<? extends Symbol> projectionOutputs = AbstractIndexWriterProjection.OUTPUTS;
        if (copyFrom instanceof AnalyzedCopyFromReturnSummary returnSummary) {
            final InputColumn sourceUriSymbol = new InputColumn(toCollect.size(), DataTypes.STRING);
            toCollect.add(SourceUriExpression.getReferenceForRelation(table.ident()));

            final InputColumn sourceUriFailureSymbol = new InputColumn(toCollect.size(), DataTypes.STRING);
            toCollect.add(SourceUriFailureExpression.getReferenceForRelation(table.ident()));

            final InputColumn lineNumberSymbol = new InputColumn(toCollect.size(), DataTypes.LONG);
            toCollect.add(SourceLineNumberExpression.getReferenceForRelation(table.ident()));

            final InputColumn sourceParsingFailureSymbol = new InputColumn(toCollect.size(), DataTypes.STRING);
            toCollect.add(SourceParsingFailureExpression.getReferenceForRelation(table.ident()));

            List<? extends Symbol> fields = returnSummary.outputs();
            projectionOutputs = InputColumns.create(fields, new InputColumns.SourceSymbols(fields));

            sourceIndexWriterProjection = new SourceIndexWriterReturnSummaryProjection(
                table.ident(),
                partitionIdent,
                table.getReference(SysColumns.RAW),
                new InputColumn(rawOrDocIdx, rawOrDoc.valueType()),
                table.primaryKey(),
                InputColumns.create(table.partitionedByColumns(), sourceSymbols),
                clusteredBy,
                boundedCopyFrom.settings(),
                excludes,
                InputColumns.create(primaryKeyRefs, sourceSymbols),
                clusteredByInputCol,
                projectionOutputs,
                table.isPartitioned(), // autoCreateIndices
                sourceUriSymbol,
                sourceUriFailureSymbol,
                sourceParsingFailureSymbol,
                lineNumberSymbol
            );
        } else {
            sourceIndexWriterProjection = new SourceIndexWriterProjection(
                table.ident(),
                partitionIdent,
                table.getReference(SysColumns.RAW),
                new InputColumn(rawOrDocIdx, rawOrDoc.valueType()),
                table.primaryKey(),
                InputColumns.create(table.partitionedByColumns(), sourceSymbols),
                clusteredBy,
                boundedCopyFrom.settings(),
                excludes,
                InputColumns.create(primaryKeyRefs, sourceSymbols),
                clusteredByInputCol,
                projectionOutputs,
                table.isPartitioned() // autoCreateIndices
            );
        }

        // if there are partitionValues (we've had a PARTITION clause in the statement)
        // we need to use the calculated partition values because the partition columns are likely NOT in the data being read
        // the partitionedBy-inputColumns created for the projection are still valid because the positions are not changed
        if (partitionValues != null) {
            rewriteToCollectToUsePartitionValues(table.partitionedByColumns(), partitionValues, toCollect);
        }

        Integer numReaders = NUM_READERS_SETTING.getOrNull(boundedCopyFrom.settings());
        numReaders = numReaders == null ? allNodes.getSize() : numReaders;
        FileUriCollectPhase collectPhase = new FileUriCollectPhase(
            context.jobId(),
            context.nextExecutionPhaseId(),
            "copyFrom",
            getExecutionNodes(
                allNodes,
                numReaders,
                boundedCopyFrom.nodePredicate()),
            boundedCopyFrom.uri(),
            boundedCopyFrom.targetColumns(),
            toCollect,
            Collections.emptyList(),
            COMPRESSION_SETTING.getOrNull(boundedCopyFrom.settings()),
            SHARED_SETTING.getOrNull(boundedCopyFrom.settings()),
            CopyFromParserProperties.of(boundedCopyFrom.settings()),
            boundedCopyFrom.inputFormat(),
            boundedCopyFrom.settings()
        );

        Collect collect = new Collect(collectPhase, LimitAndOffset.NO_LIMIT, 0, 1, -1, null);
        // add the projection to the plan to ensure that the outputs are correctly set to the projection outputs
        collect.addProjection(sourceIndexWriterProjection);

        List<Projection> handlerProjections;
        if (copyFrom instanceof AnalyzedCopyFromReturnSummary) {
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
            int idx;
            if (partitionedByColumn instanceof GeneratedReference genRef) {
                idx = toCollect.indexOf(genRef.generatedExpression());
            } else {
                idx = toCollect.indexOf(partitionedByColumn);
            }
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
    private static List<Symbol> getSymbolsRequiredForShardIdCalc(List<Reference> primaryKeyRefs,
                                                                 List<Reference> partitionedByRefs,
                                                                 @Nullable Reference clusteredBy) {
        HashSet<Symbol> toCollectUnique = new HashSet<>();
        primaryKeyRefs.forEach(r -> addWithRefDependencies(toCollectUnique, r));
        partitionedByRefs.forEach(r -> addWithRefDependencies(toCollectUnique, r));
        if (clusteredBy != null) {
            addWithRefDependencies(toCollectUnique, clusteredBy);
        }
        return new ArrayList<>(toCollectUnique);
    }

    private static void addWithRefDependencies(HashSet<Symbol> toCollectUnique, Reference ref) {
        if (ref instanceof GeneratedReference generatedReference) {
            toCollectUnique.add(generatedReference.generatedExpression());
        } else {
            toCollectUnique.add(ref);
        }
    }

    /**
     * Return RAW or DOC Reference:
     *
     * Copy from has two "modes" on how the json-object-lines are processed:
     *
     * 1: non-partitioned tables or partitioned tables with partition ident --> import into single es index
     *    -> collect raw source and import as is
     *
     * 2: partitioned table without partition ident
     *    -> collect document and partition by values
     *    -> exclude partitioned by columns from document
     *    -> insert into es index (partition determined by partition by value)
     */
    private static Reference rawOrDoc(DocTableInfo table, String selectedPartitionIdent) {
        if (table.isPartitioned() && selectedPartitionIdent == null) {
            return table.getReference(SysColumns.DOC);
        }
        return table.getReference(SysColumns.RAW);
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

    /**
     * Validates that uri is either String or List<String>.
     *
     * If schema is "file" also validates scheme independent settings.
     *
     * Settings of other schemes are validated later in plugins
     * as only plugins are aware of scheme specific properties.
     */
    private static Literal<?> validateAndConvertToLiteral(Object uri, GenericProperties<Object> properties) {
        if (uri instanceof String) {
            String uriAsString = DataTypes.STRING.sanitizeValue(uri);
            if (uriAsString.startsWith("/") || uriAsString.startsWith("file:")) {
                properties.ensureContainsOnly(CopyStatementSettings.COMMON_COPY_FROM_SETTINGS);
            }
            return Literal.of(uriAsString);
        } else if (uri instanceof List<?> uris) {
            Object value = uris.get(0);
            if (!(value instanceof String uriAsString)) {
                throw AnalyzedCopyFrom.raiseInvalidType(DataTypes.guessType(uri));
            }
            if (uriAsString.startsWith("/") || uriAsString.startsWith("file:")) {
                properties.ensureContainsOnly(CopyStatementSettings.COMMON_COPY_FROM_SETTINGS);
            }
            return Literal.of(DataTypes.STRING_ARRAY, DataTypes.STRING_ARRAY.sanitizeValue(uri));
        }
        throw AnalyzedCopyFrom.raiseInvalidType(DataTypes.guessType(uri));
    }

    private static Predicate<DiscoveryNode> discoveryNodePredicate(@Nullable Object nodeFilter) {
        if (nodeFilter == null) {
            return ignoredDiscoveryNode -> true;
        }
        try {
            return NodeFilters.fromMap((Map<?, ?>) nodeFilter);
        } catch (ClassCastException e) {
            throw new IllegalArgumentException(String.format(
                Locale.ENGLISH,
                "Invalid parameter passed to %s. Expected an object with name or id keys and string values. Got '%s'",
                NodeFilters.NAME, nodeFilter));
        }
    }
}
