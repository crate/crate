/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.planner.statement;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.google.common.annotations.VisibleForTesting;
import io.crate.analyze.CopyFromAnalyzedStatement;
import io.crate.analyze.CopyFromReturnSummaryAnalyzedStatement;
import io.crate.analyze.CopyToAnalyzedStatement;
import io.crate.common.collections.Lists2;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.execution.dsl.phases.FileUriCollectPhase;
import io.crate.execution.dsl.phases.NodeOperationTree;
import io.crate.execution.dsl.projection.AbstractIndexWriterProjection;
import io.crate.execution.dsl.projection.MergeCountProjection;
import io.crate.execution.dsl.projection.Projection;
import io.crate.execution.dsl.projection.SourceIndexWriterProjection;
import io.crate.execution.dsl.projection.SourceIndexWriterReturnSummaryProjection;
import io.crate.execution.dsl.projection.WriterProjection;
import io.crate.execution.dsl.projection.builder.InputColumns;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.execution.engine.NodeOperationTreeGenerator;
import io.crate.execution.engine.pipeline.TopN;
import io.crate.expression.reference.file.SourceLineNumberExpression;
import io.crate.expression.reference.file.SourceUriExpression;
import io.crate.expression.reference.file.SourceUriFailureExpression;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.GeneratedReference;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.Merge;
import io.crate.planner.Plan;
import io.crate.planner.PlannerContext;
import io.crate.planner.SubqueryPlanner;
import io.crate.planner.consumer.FetchMode;
import io.crate.planner.node.dql.Collect;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.operators.LogicalPlanner;
import io.crate.planner.operators.SubQueryResults;
import io.crate.types.DataTypes;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public final class CopyStatementPlanner {

    private CopyStatementPlanner() {
    }

    public static Plan planCopyFrom(CopyFromAnalyzedStatement copyFrom) {
        return new CopyFrom(copyFrom);
    }

    public static Plan planCopyTo(CopyToAnalyzedStatement statement,
                                  LogicalPlanner logicalPlanner,
                                  SubqueryPlanner subqueryPlanner) {
        return new CopyTo(statement, logicalPlanner, subqueryPlanner);
    }

    static class CopyTo implements Plan {

        @VisibleForTesting
        final CopyToAnalyzedStatement copyTo;

        @VisibleForTesting
        final LogicalPlanner logicalPlanner;

        @VisibleForTesting
        final SubqueryPlanner subqueryPlanner;

        CopyTo(CopyToAnalyzedStatement copyTo, LogicalPlanner logicalPlanner, SubqueryPlanner subqueryPlanner) {
            this.copyTo = copyTo;
            this.logicalPlanner = logicalPlanner;
            this.subqueryPlanner = subqueryPlanner;
        }

        @Override
        public StatementType type() {
            return StatementType.COPY;
        }

        @Override
        public void executeOrFail(DependencyCarrier executor,
                                  PlannerContext plannerContext,
                                  RowConsumer consumer,
                                  Row params,
                                  SubQueryResults subQueryResults) {
            ExecutionPlan executionPlan = planCopyToExecution(
                copyTo, plannerContext, logicalPlanner, subqueryPlanner, executor.projectionBuilder(), params);
            NodeOperationTree nodeOpTree = NodeOperationTreeGenerator.fromPlan(executionPlan, executor.localNodeId());
            executor.phasesTaskFactory()
                .create(plannerContext.jobId(), Collections.singletonList(nodeOpTree))
                .execute(consumer, plannerContext.transactionContext());
        }
    }

    public static class CopyFrom implements Plan {

        public final CopyFromAnalyzedStatement copyFrom;

        CopyFrom(CopyFromAnalyzedStatement copyFrom) {
            this.copyFrom = copyFrom;
        }

        @Override
        public StatementType type() {
            return StatementType.COPY;
        }

        @Override
        public void executeOrFail(DependencyCarrier executor,
                                  PlannerContext plannerContext,
                                  RowConsumer consumer,
                                  Row params,
                                  SubQueryResults subQueryResults) {
            ExecutionPlan plan = planCopyFromExecution(executor.clusterService().state().nodes(), copyFrom, plannerContext);
            NodeOperationTree nodeOpTree = NodeOperationTreeGenerator.fromPlan(plan, executor.localNodeId());
            executor.phasesTaskFactory()
                .create(plannerContext.jobId(), Collections.singletonList(nodeOpTree))
                .execute(consumer, plannerContext.transactionContext());
        }
    }

    public static ExecutionPlan planCopyFromExecution(DiscoveryNodes allNodes,
                                                      CopyFromAnalyzedStatement copyFrom,
                                                      PlannerContext context) {
        /*
         * Create a plan that reads json-objects-lines from a file and then executes upsert requests to index the data
         */
        DocTableInfo table = copyFrom.table();
        String partitionIdent = copyFrom.partitionIdent();
        List<String> partitionedByNames = Collections.emptyList();
        List<String> partitionValues = Collections.emptyList();
        if (partitionIdent == null) {
            if (table.isPartitioned()) {
                partitionedByNames = Lists2.map(table.partitionedBy(), ColumnIdent::fqn);
            }
        } else {
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

        List<Symbol> toCollect = getSymbolsRequiredForShardIdCalc(
            primaryKeyRefs,
            table.partitionedByColumns(),
            clusteredBy == null ? null : table.getReference(clusteredBy)
        );
        Reference rawOrDoc = rawOrDoc(table, partitionIdent);
        final int rawOrDocIdx = toCollect.size();
        toCollect.add(rawOrDoc);


        String[] excludes = partitionedByNames.size() > 0
            ? partitionedByNames.toArray(new String[0]) : null;

        InputColumns.SourceSymbols sourceSymbols = new InputColumns.SourceSymbols(toCollect);
        Symbol clusteredByInputCol = null;
        if (clusteredBy != null) {
            clusteredByInputCol = InputColumns.create(table.getReference(clusteredBy), sourceSymbols);
        }

        SourceIndexWriterProjection sourceIndexWriterProjection;

        List<? extends Symbol> projectionOutputs = AbstractIndexWriterProjection.OUTPUTS;
        boolean isReturnSummary = copyFrom instanceof CopyFromReturnSummaryAnalyzedStatement;
        if (isReturnSummary) {
            final InputColumn sourceUriSymbol = new InputColumn(toCollect.size(), DataTypes.STRING);
            toCollect.add(SourceUriExpression.getReferenceForRelation(table.ident()));

            final InputColumn sourceUriFailureSymbol = new InputColumn(toCollect.size(), DataTypes.STRING);
            toCollect.add(SourceUriFailureExpression.getReferenceForRelation(table.ident()));

            final InputColumn lineNumberSymbol = new InputColumn(toCollect.size(), DataTypes.LONG);
            toCollect.add(SourceLineNumberExpression.getReferenceForRelation(table.ident()));

            List<? extends Symbol> fields = ((CopyFromReturnSummaryAnalyzedStatement) copyFrom).fields();
            projectionOutputs = InputColumns.create(fields, new InputColumns.SourceSymbols(fields));

            sourceIndexWriterProjection = new SourceIndexWriterReturnSummaryProjection(
                table.ident(),
                partitionIdent,
                table.getReference(DocSysColumns.RAW),
                new InputColumn(rawOrDocIdx, rawOrDoc.valueType()),
                table.primaryKey(),
                InputColumns.create(table.partitionedByColumns(), sourceSymbols),
                clusteredBy,
                copyFrom.settings(),
                null,
                excludes,
                InputColumns.create(primaryKeyRefs, sourceSymbols),
                clusteredByInputCol,
                projectionOutputs,
                table.isPartitioned(), // autoCreateIndices
                sourceUriSymbol,
                sourceUriFailureSymbol,
                lineNumberSymbol
            );
        } else {
            sourceIndexWriterProjection = new SourceIndexWriterProjection(
                table.ident(),
                partitionIdent,
                table.getReference(DocSysColumns.RAW),
                new InputColumn(rawOrDocIdx, rawOrDoc.valueType()),
                table.primaryKey(),
                InputColumns.create(table.partitionedByColumns(), sourceSymbols),
                clusteredBy,
                copyFrom.settings(),
                null,
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

        FileUriCollectPhase collectPhase = new FileUriCollectPhase(
            context.jobId(),
            context.nextExecutionPhaseId(),
            "copyFrom",
            getExecutionNodes(allNodes, copyFrom.settings().getAsInt("num_readers", allNodes.getSize()), copyFrom.nodePredicate()),
            copyFrom.uri(),
            toCollect,
            Collections.emptyList(),
            copyFrom.settings().get("compression", null),
            copyFrom.settings().getAsBoolean("shared", null),
            copyFrom.inputFormat()
        );

        Collect collect = new Collect(collectPhase, TopN.NO_LIMIT, 0, 1, -1, null);
        // add the projection to the plan to ensure that the outputs are correctly set to the projection outputs
        collect.addProjection(sourceIndexWriterProjection);

        List<Projection> handlerProjections;
        if (isReturnSummary) {
            handlerProjections = Collections.emptyList();
        } else {
            handlerProjections = Collections.singletonList(MergeCountProjection.INSTANCE);
        }
        return Merge.ensureOnHandler(collect, context, handlerProjections);
    }

    private static void rewriteToCollectToUsePartitionValues(List<Reference> partitionedByColumns,
                                                             List<String> partitionValues,
                                                             List<Symbol> toCollect) {
        for (int i = 0; i < partitionValues.size(); i++) {
            Reference partitionedByColumn = partitionedByColumns.get(i);
            int idx;
            if (partitionedByColumn instanceof GeneratedReference) {
                idx = toCollect.indexOf(((GeneratedReference) partitionedByColumn).generatedExpression());
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
        if (ref instanceof GeneratedReference) {
            toCollectUnique.add(((GeneratedReference) ref).generatedExpression());
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
            return table.getReference(DocSysColumns.DOC);
        }
        return table.getReference(DocSysColumns.RAW);
    }

    @VisibleForTesting
    static ExecutionPlan planCopyToExecution(CopyToAnalyzedStatement statement,
                                             PlannerContext context,
                                             LogicalPlanner logicalPlanner,
                                             SubqueryPlanner subqueryPlanner,
                                             ProjectionBuilder projectionBuilder,
                                             Row params) {
        WriterProjection.OutputFormat outputFormat = statement.outputFormat();
        if (outputFormat == null) {
            outputFormat = statement.columnsDefined() ?
                WriterProjection.OutputFormat.JSON_ARRAY : WriterProjection.OutputFormat.JSON_OBJECT;
        }

        WriterProjection projection = ProjectionBuilder.writerProjection(
            statement.relation().outputs(),
            statement.uri(),
            statement.compressionType(),
            statement.overwrites(),
            statement.outputNames(),
            outputFormat);

        LogicalPlan logicalPlan = logicalPlanner.normalizeAndPlan(
            statement.relation(), context, subqueryPlanner, FetchMode.NEVER_CLEAR, Set.of());
        ExecutionPlan executionPlan = logicalPlan.build(
            context, projectionBuilder, 0, 0, null, null, params, SubQueryResults.EMPTY);
        executionPlan.addProjection(projection);
        return Merge.ensureOnHandler(executionPlan, context, Collections.singletonList(MergeCountProjection.INSTANCE));
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
}
