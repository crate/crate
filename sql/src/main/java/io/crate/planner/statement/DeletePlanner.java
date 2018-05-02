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

import com.google.common.collect.ImmutableList;
import io.crate.action.sql.SessionContext;
import io.crate.analyze.AnalyzedDeleteStatement;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.collections.Lists2;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowConsumer;
import io.crate.execution.dsl.phases.NodeOperationTree;
import io.crate.execution.dsl.phases.RoutedCollectPhase;
import io.crate.execution.dsl.projection.DeleteProjection;
import io.crate.execution.dsl.projection.MergeCountProjection;
import io.crate.execution.engine.NodeOperationTreeGenerator;
import io.crate.execution.engine.pipeline.TopN;
import io.crate.execution.support.OneRowActionListener;
import io.crate.expression.eval.EvaluatingNormalizer;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.Functions;
import io.crate.metadata.IndexParts;
import io.crate.metadata.Reference;
import io.crate.metadata.Routing;
import io.crate.metadata.RoutingProvider;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.Merge;
import io.crate.planner.MultiPhasePlan;
import io.crate.planner.Plan;
import io.crate.planner.PlannerContext;
import io.crate.planner.SubqueryPlanner;
import io.crate.planner.WhereClauseOptimizer;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.node.ddl.DeleteAllPartitions;
import io.crate.planner.node.ddl.DeletePartitions;
import io.crate.planner.node.dml.DeleteById;
import io.crate.planner.node.dql.Collect;
import io.crate.types.DataTypes;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.IndicesOptions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static com.google.common.collect.Lists.newArrayList;
import static java.util.Objects.requireNonNull;

public final class DeletePlanner {

    public static Plan planDelete(Functions functions,
                                  AnalyzedDeleteStatement delete,
                                  SubqueryPlanner subqueryPlanner,
                                  PlannerContext context) {
        Plan plan = planDelete(functions, delete, context);
        return MultiPhasePlan.createIfNeeded(plan, subqueryPlanner.planSubQueries(delete));
    }

    private static Plan planDelete(Functions functions, AnalyzedDeleteStatement delete, PlannerContext context) {
        DocTableRelation tableRel = delete.relation();
        DocTableInfo table = tableRel.tableInfo();
        EvaluatingNormalizer normalizer = EvaluatingNormalizer.functionOnlyNormalizer(functions);
        WhereClauseOptimizer.DetailedQuery detailedQuery = WhereClauseOptimizer.optimize(
            normalizer, delete.query(), table, context.transactionContext());

        if (!detailedQuery.partitions().isEmpty()) {
            return new DeletePartitions(table.ident(), detailedQuery.partitions());
        }
        if (detailedQuery.docKeys().isPresent()) {
            return new DeleteById(tableRel.tableInfo(), detailedQuery.docKeys().get());
        }
        Symbol query = detailedQuery.query();
        if (table.isPartitioned() && query instanceof Input && DataTypes.BOOLEAN.value(((Input) query).value())) {
            return new DeleteAllPartitions(Lists2.copyAndReplace(table.partitions(), IndexParts::toIndexName));
        }
        return new Delete(tableRel, detailedQuery);
    }

    static class Delete implements Plan {

        private final DocTableRelation table;
        private final WhereClauseOptimizer.DetailedQuery detailedQuery;

        public Delete(DocTableRelation table, WhereClauseOptimizer.DetailedQuery detailedQuery) {
            this.table = table;
            this.detailedQuery = detailedQuery;
        }

        @Override
        public void execute(DependencyCarrier executor,
                            PlannerContext plannerContext,
                            RowConsumer consumer,
                            Row params,
                            Map<SelectSymbol, Object> valuesBySubQuery) {

            WhereClause where = detailedQuery.toBoundWhereClause(
                table.tableInfo(),
                executor.functions(),
                params,
                valuesBySubQuery,
                plannerContext.transactionContext()
            );
            if (!where.partitions().isEmpty() && !where.hasQuery()) {
                DeleteIndexRequest request = new DeleteIndexRequest(where.partitions().toArray(new String[0]));
                request.indicesOptions(IndicesOptions.lenientExpandOpen());
                executor.transportActionProvider().transportDeleteIndexAction()
                    .execute(request, new OneRowActionListener<>(consumer, o -> new Row1(-1L)));
                return;
            }

            ExecutionPlan executionPlan = deleteByQuery(table, plannerContext, where);
            NodeOperationTree nodeOpTree = NodeOperationTreeGenerator.fromPlan(executionPlan, executor.localNodeId());
            executor.phasesTaskFactory()
                .create(plannerContext.jobId(), Collections.singletonList(nodeOpTree))
                .execute(consumer);
        }

        @Override
        public List<CompletableFuture<Long>> executeBulk(DependencyCarrier executor,
                                                         PlannerContext plannerContext,
                                                         List<Row> bulkParams,
                                                         Map<SelectSymbol, Object> valuesBySubQuery) {
            ArrayList<NodeOperationTree> nodeOperationTreeList = new ArrayList<>(bulkParams.size());
            for (Row params : bulkParams) {
                WhereClause where = detailedQuery.toBoundWhereClause(
                    table.tableInfo(),
                    executor.functions(),
                    params,
                    valuesBySubQuery,
                    plannerContext.transactionContext());
                ExecutionPlan executionPlan = deleteByQuery(table, plannerContext, where);
                nodeOperationTreeList.add(NodeOperationTreeGenerator.fromPlan(executionPlan, executor.localNodeId()));
            }
            return executor.phasesTaskFactory()
                .create(plannerContext.jobId(), nodeOperationTreeList)
                .executeBulk();
        }
    }

    private static ExecutionPlan deleteByQuery(DocTableRelation table, PlannerContext context, WhereClause where) {
        DocTableInfo tableInfo = table.tableInfo();
        Reference idReference = requireNonNull(tableInfo.getReference(DocSysColumns.ID), "Table has to have a _id reference");
        DeleteProjection deleteProjection = new DeleteProjection(new InputColumn(0, idReference.valueType()));
        SessionContext sessionContext = context.transactionContext().sessionContext();
        Routing routing = context.allocateRouting(
            tableInfo, where, RoutingProvider.ShardSelection.PRIMARIES, sessionContext);
        RoutedCollectPhase collectPhase = new RoutedCollectPhase(
            context.jobId(),
            context.nextExecutionPhaseId(),
            "collect",
            routing,
            tableInfo.rowGranularity(),
            newArrayList(idReference),
            ImmutableList.of(deleteProjection),
            where.queryOrFallback(),
            DistributionInfo.DEFAULT_BROADCAST,
            sessionContext.user()
        );
        Collect collect = new Collect(collectPhase, TopN.NO_LIMIT, 0, 1, 1, null);
        return Merge.ensureOnHandler(collect, context, Collections.singletonList(MergeCountProjection.INSTANCE));
    }
}
