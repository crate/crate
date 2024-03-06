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

package io.crate.planner.consumer;

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.jetbrains.annotations.Nullable;

import org.elasticsearch.Version;

import io.crate.analyze.AnalyzedUpdateStatement;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.AbstractTableRelation;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.relations.TableRelation;
import org.jetbrains.annotations.VisibleForTesting;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.exceptions.VersioningValidationException;
import io.crate.execution.dsl.phases.NodeOperationTree;
import io.crate.execution.dsl.phases.RoutedCollectPhase;
import io.crate.execution.dsl.projection.MergeCountProjection;
import io.crate.execution.dsl.projection.Projection;
import io.crate.execution.dsl.projection.SysUpdateProjection;
import io.crate.execution.dsl.projection.UpdateProjection;
import io.crate.execution.engine.NodeOperationTreeGenerator;
import io.crate.execution.engine.pipeline.LimitAndOffset;
import io.crate.expression.eval.EvaluatingNormalizer;
import io.crate.expression.symbol.Assignments;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.Reference;
import io.crate.metadata.Routing;
import io.crate.metadata.RoutingProvider;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.Merge;
import io.crate.planner.MultiPhasePlan;
import io.crate.planner.Plan;
import io.crate.planner.PlannerContext;
import io.crate.planner.SubqueryPlanner;
import io.crate.planner.WhereClauseOptimizer;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.node.dml.UpdateById;
import io.crate.planner.node.dql.Collect;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.operators.SubQueryAndParamBinder;
import io.crate.planner.operators.SubQueryResults;
import io.crate.planner.optimizer.symbol.Optimizer;
import io.crate.types.DataTypes;

public final class UpdatePlanner {

    public static final String RETURNING_VERSION_ERROR_MSG =
        "Returning clause for Update is only supported when all nodes in the cluster running at least version 4.2.0";

    private UpdatePlanner() {
    }

    public static Plan plan(AnalyzedUpdateStatement update,
                            PlannerContext plannerCtx,
                            SubqueryPlanner subqueryPlanner) {

        if (update.outputs() != null &&
            !plannerCtx.clusterState().nodes().getMinNodeVersion().onOrAfter(Version.V_4_2_0)) {
            throw new UnsupportedFeatureException(RETURNING_VERSION_ERROR_MSG);
        }

        AbstractTableRelation<?> table = update.table();

        Plan plan;
        if (table instanceof DocTableRelation) {
            DocTableRelation docTable = (DocTableRelation) table;
            plan = plan(docTable,
                        update.assignmentByTargetCol(),
                        update.query(),
                        plannerCtx,
                        update.outputs());
        } else {
            plan = new Update((plannerContext, params, subQueryValues) ->
                sysUpdate(
                    plannerContext,
                    (TableRelation) table,
                    update.assignmentByTargetCol(),
                    update.query(),
                    params,
                    subQueryValues,
                    update.outputs()
                )
            );
        }
        Map<LogicalPlan, SelectSymbol> subQueries = subqueryPlanner.planSubQueries(update).uncorrelated();
        return MultiPhasePlan.createIfNeeded(plan, subQueries);
    }

    private static Plan plan(DocTableRelation docTable,
                             LinkedHashMap<Reference, Symbol> assignmentByTargetCol,
                             Symbol query,
                             PlannerContext plannerCtx,
                             @Nullable List<Symbol> returnValues) {
        DocTableInfo tableInfo = docTable.tableInfo();
        EvaluatingNormalizer normalizer = EvaluatingNormalizer.functionOnlyNormalizer(plannerCtx.nodeContext());
        WhereClauseOptimizer.DetailedQuery detailedQuery = WhereClauseOptimizer.optimize(
            normalizer, query, tableInfo, plannerCtx.transactionContext(), plannerCtx.nodeContext());

        if (detailedQuery.docKeys().isPresent() && detailedQuery.queryHasPkSymbolsOnly()) {
            return new UpdateById(
                tableInfo,
                assignmentByTargetCol,
                detailedQuery.docKeys().get(),
                returnValues,
                plannerCtx.nodeContext()
            );
        }

        return new Update((plannerContext, params, subQueryValues) ->
                              updateByQuery(plannerContext,
                                            docTable,
                                            assignmentByTargetCol,
                                            detailedQuery,
                                            params,
                                            subQueryValues,
                                            returnValues));
    }

    @FunctionalInterface
    public interface CreateExecutionPlan {
        ExecutionPlan create(PlannerContext plannerCtx, Row params, SubQueryResults subQueryResults);
    }

    public static class Update implements Plan {

        @VisibleForTesting
        public final CreateExecutionPlan createExecutionPlan;

        Update(CreateExecutionPlan createExecutionPlan) {
            this.createExecutionPlan = createExecutionPlan;
        }

        @Override
        public StatementType type() {
            return StatementType.UPDATE;
        }

        @Override
        public void executeOrFail(DependencyCarrier executor,
                                  PlannerContext plannerContext,
                                  RowConsumer consumer,
                                  Row params,
                                  SubQueryResults subQueryResults) throws Exception {
            ExecutionPlan executionPlan = createExecutionPlan.create(plannerContext, params, subQueryResults);
            NodeOperationTree nodeOpTree = NodeOperationTreeGenerator.fromPlan(executionPlan, executor.localNodeId());

            executor.phasesTaskFactory()
                .create(plannerContext.jobId(), singletonList(nodeOpTree))
                .execute(consumer, plannerContext.transactionContext());
        }

        @Override
        public List<CompletableFuture<Long>> executeBulk(DependencyCarrier executor,
                                                         PlannerContext plannerContext,
                                                         List<Row> bulkParams,
                                                         SubQueryResults subQueryResults) {
            List<NodeOperationTree> nodeOpTreeList = new ArrayList<>(bulkParams.size());
            for (Row params : bulkParams) {
                ExecutionPlan executionPlan = createExecutionPlan.create(plannerContext, params, subQueryResults);
                nodeOpTreeList.add(NodeOperationTreeGenerator.fromPlan(executionPlan, executor.localNodeId()));
            }
            return executor.phasesTaskFactory()
                .create(plannerContext.jobId(), nodeOpTreeList)
                .executeBulk(plannerContext.transactionContext());
        }
    }

    private static ExecutionPlan sysUpdate(PlannerContext plannerContext,
                                           TableRelation table,
                                           Map<Reference, Symbol> assignmentByTargetCol,
                                           Symbol query,
                                           Row params,
                                           SubQueryResults subQueryResults,
                                           @Nullable List<Symbol> returnValues) {
        TableInfo tableInfo = table.tableInfo();
        Reference idReference = requireNonNull(tableInfo.getReference(DocSysColumns.ID), "Table must have a _id column");
        Symbol[] outputSymbols;
        if (returnValues == null) {
            outputSymbols = new Symbol[]{new InputColumn(0, DataTypes.LONG)};
        } else {
            outputSymbols = new Symbol[returnValues.size()];
            for (int i = 0; i < returnValues.size(); i++) {
                outputSymbols[i] = new InputColumn(i, returnValues.get(i).valueType());
            }
        }
        SysUpdateProjection updateProjection = new SysUpdateProjection(
            new InputColumn(0, idReference.valueType()),
            assignmentByTargetCol,
            outputSymbols,
            returnValues == null ? null : returnValues.toArray(new Symbol[0])
        );
        WhereClause where = new WhereClause(SubQueryAndParamBinder.convert(query, params, subQueryResults));

        if (returnValues == null) {
            return createCollectAndMerge(plannerContext,
                                         tableInfo,
                                         idReference,
                                         updateProjection,
                                         where,
                                         1,
                                         1,
                                         MergeCountProjection.INSTANCE);
        } else {
            return createCollectAndMerge(plannerContext,
                                         tableInfo,
                                         idReference,
                                         updateProjection,
                                         where,
                                         updateProjection.outputs().size(),
                                         -1
            );
        }
    }

    private static ExecutionPlan updateByQuery(PlannerContext plannerCtx,
                                               DocTableRelation table,
                                               LinkedHashMap<Reference, Symbol> assignmentByTargetCol,
                                               WhereClauseOptimizer.DetailedQuery detailedQuery,
                                               Row params,
                                               SubQueryResults subQueryResults,
                                               @Nullable List<Symbol> returnValues) {
        DocTableInfo tableInfo = table.tableInfo();
        Reference idReference = requireNonNull(tableInfo.getReference(DocSysColumns.ID),
                                               "Table must have a _id column");
        Assignments assignments = Assignments.convert(assignmentByTargetCol, plannerCtx.nodeContext());
        Symbol[] assignmentSources = assignments.bindSources(tableInfo, params, subQueryResults);
        Symbol[] outputSymbols;
        if (returnValues == null) {
            //When there are no return values, set the output to a long representing the count of updated rows
            outputSymbols = new Symbol[]{new InputColumn(0, DataTypes.LONG)};
        } else {
            outputSymbols = new Symbol[returnValues.size()];
            for (int i = 0; i < returnValues.size(); i++) {
                outputSymbols[i] = new InputColumn(i, returnValues.get(i).valueType());
            }
        }
        UpdateProjection updateProjection = new UpdateProjection(
            new InputColumn(0, idReference.valueType()),
            assignments.targetNames(),
            assignmentSources,
            outputSymbols,
            returnValues == null ? null : returnValues.toArray(new Symbol[0]),
            null);

        WhereClause where = detailedQuery.toBoundWhereClause(
            tableInfo, params, subQueryResults, plannerCtx.transactionContext(), plannerCtx.nodeContext());
        if (where.hasVersions()) {
            throw VersioningValidationException.versionInvalidUsage();
        } else if (where.hasSeqNoAndPrimaryTerm()) {
            throw VersioningValidationException.seqNoAndPrimaryTermUsage();
        }

        if (returnValues == null) {
            return createCollectAndMerge(plannerCtx,
                                         tableInfo,
                                         idReference,
                                         updateProjection,
                                         where,
                                         1,
                                         1,
                                         MergeCountProjection.INSTANCE);
        } else {
            return createCollectAndMerge(plannerCtx,
                                         tableInfo,
                                         idReference,
                                         updateProjection,
                                         where,
                                         updateProjection.outputs().size(),
                                         -1
                                         );
        }
    }

    private static ExecutionPlan createCollectAndMerge(PlannerContext plannerCtx,
                                                       TableInfo tableInfo,
                                                       Reference idReference,
                                                       Projection updateProjection,
                                                       WhereClause where,
                                                       int numOutPuts,
                                                       int maxRowsPerNode,
                                                       Projection ... mergeProjections
                                                       ) {
        var sessionSettings = plannerCtx.transactionContext().sessionSettings();
        Routing routing = plannerCtx.allocateRouting(
            tableInfo, where, RoutingProvider.ShardSelection.PRIMARIES, sessionSettings);
        RoutedCollectPhase collectPhase = new RoutedCollectPhase(
            plannerCtx.jobId(),
            plannerCtx.nextExecutionPhaseId(),
            "collect",
            routing,
            tableInfo.rowGranularity(),
            List.of(idReference),
            singletonList(updateProjection),
            Optimizer.optimizeCasts(where.queryOrFallback(), plannerCtx),
            DistributionInfo.DEFAULT_BROADCAST
        );
        Collect collect = new Collect(collectPhase, LimitAndOffset.NO_LIMIT, 0, numOutPuts, maxRowsPerNode, null);
        return Merge.ensureOnHandler(collect, plannerCtx, List.of(mergeProjections));
    }
}
