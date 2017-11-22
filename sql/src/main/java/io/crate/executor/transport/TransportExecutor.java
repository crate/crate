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

package io.crate.executor.transport;

import io.crate.action.job.ContextPreparer;
import io.crate.action.sql.DCLStatementDispatcher;
import io.crate.action.sql.DDLStatementDispatcher;
import io.crate.analyze.EvaluatingNormalizer;
import io.crate.analyze.symbol.SelectSymbol;
import io.crate.data.CollectingRowConsumer;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.executor.Executor;
import io.crate.executor.Task;
import io.crate.executor.task.ExplainTask;
import io.crate.executor.task.FunctionDispatchTask;
import io.crate.executor.task.NoopTask;
import io.crate.executor.task.SetSessionTask;
import io.crate.executor.transport.ddl.TransportDropTableAction;
import io.crate.executor.transport.executionphases.ExecutionPhasesTask;
import io.crate.executor.transport.task.DeleteByIdTask;
import io.crate.executor.transport.task.DropTableTask;
import io.crate.executor.transport.task.KillJobTask;
import io.crate.executor.transport.task.KillTask;
import io.crate.executor.transport.task.LegacyUpsertByIdTask;
import io.crate.executor.transport.task.ShowCreateTableTask;
import io.crate.executor.transport.task.UpdateByIdTask;
import io.crate.executor.transport.task.elasticsearch.CreateAnalyzerTask;
import io.crate.executor.transport.task.elasticsearch.DeleteAllPartitionsTask;
import io.crate.executor.transport.task.elasticsearch.DeletePartitionTask;
import io.crate.executor.transport.task.elasticsearch.ESClusterUpdateSettingsTask;
import io.crate.executor.transport.task.elasticsearch.ESGetTask;
import io.crate.jobs.JobContextService;
import io.crate.metadata.Functions;
import io.crate.operation.InputFactory;
import io.crate.operation.NodeJobsCounter;
import io.crate.operation.NodeOperationTree;
import io.crate.operation.collect.sources.SystemCollectSource;
import io.crate.operation.projectors.ProjectionToProjectorVisitor;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.ExecutionPlanVisitor;
import io.crate.planner.Merge;
import io.crate.planner.MultiPhasePlan;
import io.crate.planner.NoopPlan;
import io.crate.planner.Plan;
import io.crate.planner.PlannerContext;
import io.crate.planner.node.dcl.GenericDCLPlan;
import io.crate.planner.node.ddl.CreateAnalyzerPlan;
import io.crate.planner.node.ddl.DeleteAllPartitions;
import io.crate.planner.node.ddl.DeletePartitions;
import io.crate.planner.node.ddl.DropTablePlan;
import io.crate.planner.node.ddl.ESClusterUpdateSettingsPlan;
import io.crate.planner.node.ddl.GenericDDLPlan;
import io.crate.planner.node.dml.DeleteById;
import io.crate.planner.node.dml.LegacyUpsertById;
import io.crate.planner.node.dml.UpdateById;
import io.crate.planner.node.dml.Upsert;
import io.crate.planner.node.dql.ESGet;
import io.crate.planner.node.dql.QueryThenFetch;
import io.crate.planner.node.dql.join.NestedLoop;
import io.crate.planner.node.management.ExplainPlan;
import io.crate.planner.node.management.KillPlan;
import io.crate.planner.node.management.ShowCreateTablePlan;
import io.crate.planner.projection.builder.ProjectionBuilder;
import io.crate.planner.statement.SetSessionPlan;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static io.crate.analyze.symbol.SelectSymbol.ResultType.SINGLE_COLUMN_MULTIPLE_VALUES;
import static io.crate.analyze.symbol.SelectSymbol.ResultType.SINGLE_COLUMN_SINGLE_VALUE;

@Singleton
public class TransportExecutor implements Executor {

    private static final Logger LOGGER = Loggers.getLogger(TransportExecutor.class);
    private static final BulkNodeOperationTreeGenerator BULK_NODE_OPERATION_VISITOR = new BulkNodeOperationTreeGenerator();

    private final ThreadPool threadPool;
    private final Functions functions;
    private final TaskCollectingVisitorExecution plan2TaskVisitor;
    private final DCLStatementDispatcher dclStatementDispatcher;
    private final DDLStatementDispatcher ddlAnalysisDispatcherProvider;

    private final ClusterService clusterService;
    private final JobContextService jobContextService;
    private final ContextPreparer contextPreparer;
    private final TransportActionProvider transportActionProvider;
    private final IndicesService indicesService;
    private final TransportDropTableAction transportDropTableAction;

    private final ProjectionToProjectorVisitor globalProjectionToProjectionVisitor;
    private final MultiPhaseExecutor multiPhaseExecutor = new MultiPhaseExecutor();
    private final ProjectionBuilder projectionBuilder;

    @Inject
    public TransportExecutor(Settings settings,
                             JobContextService jobContextService,
                             ContextPreparer contextPreparer,
                             TransportActionProvider transportActionProvider,
                             ThreadPool threadPool,
                             Functions functions,
                             DDLStatementDispatcher ddlAnalysisDispatcherProvider,
                             ClusterService clusterService,
                             NodeJobsCounter nodeJobsCounter,
                             IndicesService indicesService,
                             SystemCollectSource systemCollectSource,
                             DCLStatementDispatcher dclStatementDispatcher,
                             TransportDropTableAction transportDropTableAction) {
        this.jobContextService = jobContextService;
        this.contextPreparer = contextPreparer;
        this.transportActionProvider = transportActionProvider;
        this.threadPool = threadPool;
        this.functions = functions;
        this.ddlAnalysisDispatcherProvider = ddlAnalysisDispatcherProvider;
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.dclStatementDispatcher = dclStatementDispatcher;
        this.transportDropTableAction = transportDropTableAction;
        this.plan2TaskVisitor = new TaskCollectingVisitorExecution();
        this.projectionBuilder = new ProjectionBuilder(functions);
        EvaluatingNormalizer normalizer = EvaluatingNormalizer.functionOnlyNormalizer(functions);
        globalProjectionToProjectionVisitor = new ProjectionToProjectorVisitor(
            clusterService,
            nodeJobsCounter,
            functions,
            threadPool,
            settings,
            transportActionProvider,
            new InputFactory(functions),
            normalizer,
            systemCollectSource::getRowUpdater,
            systemCollectSource::tableDefinition
            );
    }

    @Override
    public void execute(Plan plan, PlannerContext plannerContext, RowConsumer consumer, Row parameters) {
        ExecutionPlan executionPlan = plan.build(plannerContext, projectionBuilder, parameters);
        CompletableFuture<ExecutionPlan> planFuture = multiPhaseExecutor.process(executionPlan, plannerContext);
        planFuture
            .thenAccept(p -> plan2TaskVisitor.process(p, plannerContext).execute(consumer, parameters))
            .exceptionally(t -> {
                consumer.accept(null, t);
                return null;
            });
    }

    @Override
    public List<CompletableFuture<Long>> executeBulk(Plan plan, PlannerContext plannerContext, List<Row> bulkParams) {
        Task task = createTask(plan, plannerContext, bulkParams);
        return task.executeBulk(bulkParams);
    }

    private Task createTask(Plan plan, PlannerContext plannerCtx, List<Row> bulkParams) {
        // this is a bit in-progress here; We should be able to clean this up once all statement analyzers are unbound

        if (plan instanceof Upsert || plan instanceof LegacyUpsertById) {
            // These are legacy plans which can already bulk specialized.
            // They should eventually be adopted, so that the Plan itself is params independent.
            return plan2TaskVisitor.process(plan.build(plannerCtx, projectionBuilder, Row.EMPTY), plannerCtx);
        } else if (plan instanceof DeleteById) {
            return new DeleteByIdTask(
                clusterService, functions, transportActionProvider.transportShardDeleteAction(), ((DeleteById) plan));
        } else if (plan instanceof UpdateById) {
            return new UpdateByIdTask(
                clusterService, functions, transportActionProvider.transportShardUpsertAction(), ((UpdateById) plan));
        }
        // Execution plans can be param specific, but we still want to execute all together in a single task/jobRequest
        String localNodeId = clusterService.localNode().getId();
        ArrayList<NodeOperationTree> nodeOpTreeList = new ArrayList<>();
        for (Row params : bulkParams) {
            ExecutionPlan executionPlan = plan.build(plannerCtx, projectionBuilder, params);
            NodeOperationTree nodeOpTree = NodeOperationTreeGenerator.fromPlan(executionPlan, localNodeId);
            nodeOpTreeList.add(nodeOpTree);
        }
        return new ExecutionPhasesTask(
            plannerCtx.jobId(),
            clusterService,
            contextPreparer,
            jobContextService,
            indicesService,
            transportActionProvider.transportJobInitAction(),
            transportActionProvider.transportKillJobsNodeAction(),
            nodeOpTreeList
        );
    }

    private class TaskCollectingVisitorExecution extends ExecutionPlanVisitor<PlannerContext, Task> {

        @Override
        public Task visitNoopPlan(NoopPlan plan, PlannerContext context) {
            return NoopTask.INSTANCE;
        }

        @Override
        public Task visitSetSessionPlan(SetSessionPlan plan, PlannerContext context) {
            return new SetSessionTask(plan.jobId(), plan.settings(), plan.sessionContext());
        }

        @Override
        public Task visitExplainPlan(ExplainPlan explainPlan, PlannerContext context) {
            ExecutionPlan subExecutionPlan = explainPlan.subPlan().build(context, projectionBuilder, Row.EMPTY);
            return new ExplainTask(subExecutionPlan);
        }

        @Override
        protected Task visitPlan(ExecutionPlan executionPlan, PlannerContext context) {
            return executionPhasesTask(executionPlan);
        }

        private ExecutionPhasesTask executionPhasesTask(ExecutionPlan executionPlan) {
            List<NodeOperationTree> nodeOperationTrees = BULK_NODE_OPERATION_VISITOR.createNodeOperationTrees(
                executionPlan, clusterService.localNode().getId());
            LOGGER.debug("Created NodeOperationTrees from Plan: {}", nodeOperationTrees);
            return new ExecutionPhasesTask(
                executionPlan.jobId(),
                clusterService,
                contextPreparer,
                jobContextService,
                indicesService,
                transportActionProvider.transportJobInitAction(),
                transportActionProvider.transportKillJobsNodeAction(),
                nodeOperationTrees
            );
        }

        @Override
        public Task visitGetPlan(ESGet plan, PlannerContext context) {
            return new ESGetTask(
                functions,
                globalProjectionToProjectionVisitor,
                transportActionProvider,
                plan,
                jobContextService);
        }

        @Override
        public Task visitDropTablePlan(DropTablePlan plan, PlannerContext context) {
            return new DropTableTask(plan, transportDropTableAction);
        }

        @Override
        public Task visitKillPlan(KillPlan killPlan, PlannerContext context) {
            return killPlan.jobToKill().isPresent() ?
                new KillJobTask(transportActionProvider.transportKillJobsNodeAction(),
                    killPlan.jobId(),
                    killPlan.jobToKill().get()) :
                new KillTask(transportActionProvider.transportKillAllNodeAction(), killPlan.jobId());
        }

        @Override
        public Task visitShowCreateTable(ShowCreateTablePlan showCreateTablePlan, PlannerContext context) {
            return new ShowCreateTableTask(showCreateTablePlan.statement().tableInfo());
        }

        @Override
        public Task visitGenericDDLPLan(GenericDDLPlan genericDDLPlan, PlannerContext context) {
            return new FunctionDispatchTask(genericDDLPlan.jobId(), ddlAnalysisDispatcherProvider, genericDDLPlan.statement());
        }

        @Override
        public Task visitGenericDCLPlan(GenericDCLPlan genericDCLPlan, PlannerContext context) {
            return new FunctionDispatchTask(genericDCLPlan.jobId(), dclStatementDispatcher, genericDCLPlan.statement());
        }

        @Override
        public Task visitESClusterUpdateSettingsPlan(ESClusterUpdateSettingsPlan plan, PlannerContext context) {
            return new ESClusterUpdateSettingsTask(plan, transportActionProvider.transportClusterUpdateSettingsAction());
        }

        @Override
        public Task visitCreateAnalyzerPlan(CreateAnalyzerPlan plan, PlannerContext context) {
            return new CreateAnalyzerTask(plan, transportActionProvider.transportClusterUpdateSettingsAction());
        }

        @Override
        public Task visitDeleteById(DeleteById plan, PlannerContext context) {
            return new DeleteByIdTask(
                clusterService, functions, transportActionProvider.transportShardDeleteAction(), plan);
        }

        @Override
        public Task visitUpdateById(UpdateById updateById, PlannerContext context) {
            return new UpdateByIdTask(
                clusterService, functions, transportActionProvider.transportShardUpsertAction(), updateById);
        }

        @Override
        public Task visitLegacyUpsertById(LegacyUpsertById plan, PlannerContext context) {
            return new LegacyUpsertByIdTask(
                plan,
                clusterService,
                threadPool.scheduler(),
                clusterService.state().metaData().settings(),
                transportActionProvider.transportShardUpsertAction()::execute,
                transportActionProvider.transportBulkCreateIndicesAction());
        }

        @Override
        public Task visitDeletePartitions(DeletePartitions plan, PlannerContext context) {
            return new DeletePartitionTask(plan, functions, transportActionProvider.transportDeleteIndexAction());
        }

        @Override
        public Task visitDeleteAllPartitions(DeleteAllPartitions plan, PlannerContext context) {
            return new DeleteAllPartitionsTask(plan, transportActionProvider.transportDeleteIndexAction());
        }

        @Override
        public Task visitMultiPhasePlan(MultiPhasePlan multiPhasePlan, final PlannerContext context) {
            throw new UnsupportedOperationException("MultiPhasePlan should have been processed by MultiPhaseExecutor");
        }
    }

    /**
     * Executor that triggers the execution of MultiPhasePlans.
     * E.g.:
     *
     * processing a Plan that looks as follows:
     * <pre>
     *     QTF
     *      |
     *      +-- MultiPhasePlan
     *              root: Collect3
     *              deps: [Collect1, Collect2]
     * </pre>
     *
     * Executions will be triggered for collect1 and collect2, and if those are completed, Collect3 will be returned
     * as future which encapsulated the execution
     */
    private class MultiPhaseExecutor extends ExecutionPlanVisitor<PlannerContext, CompletableFuture<ExecutionPlan>> {

        @Override
        protected CompletableFuture<ExecutionPlan> visitPlan(ExecutionPlan executionPlan, PlannerContext context) {
            return CompletableFuture.completedFuture(executionPlan);
        }

        @Override
        public CompletableFuture<ExecutionPlan> visitMerge(Merge merge, PlannerContext context) {
            return process(merge.subPlan(), context).thenApply(p -> merge);
        }

        @Override
        public CompletableFuture<ExecutionPlan> visitNestedLoop(NestedLoop plan, PlannerContext context) {
            CompletableFuture<ExecutionPlan> fLeft = process(plan.left(), context);
            CompletableFuture<ExecutionPlan> fRight = process(plan.right(), context);
            return CompletableFuture.allOf(fLeft, fRight).thenApply(x -> plan);
        }

        @Override
        public CompletableFuture<ExecutionPlan> visitQueryThenFetch(QueryThenFetch qtf, PlannerContext context) {
            return process(qtf.subPlan(), context).thenApply(x -> qtf);
        }

        @Override
        public CompletableFuture<ExecutionPlan> visitMultiPhasePlan(MultiPhasePlan multiPhasePlan, PlannerContext context) {
            Map<ExecutionPlan, SelectSymbol> dependencies = multiPhasePlan.dependencies();
            List<CompletableFuture<?>> dependencyFutures = new ArrayList<>();
            ExecutionPlan rootExecutionPlan = multiPhasePlan.rootPlan();
            for (Map.Entry<ExecutionPlan, SelectSymbol> entry : dependencies.entrySet()) {
                ExecutionPlan executionPlan = entry.getKey();
                SelectSymbol selectSymbol = entry.getValue();
                SelectSymbol.ResultType resultType = selectSymbol.getResultType();

                final CollectingRowConsumer<?, ?> consumer;
                if (resultType == SINGLE_COLUMN_SINGLE_VALUE) {
                    consumer = FirstColumnConsumers.createSingleRowConsumer();
                } else if (resultType == SINGLE_COLUMN_MULTIPLE_VALUES) {
                    consumer = FirstColumnConsumers.createAllRowsConsumer();
                } else {
                    throw new IllegalStateException("Can't create consumer: Unknown ResultType");
                }

                SubSelectSymbolReplacer replacer = new SubSelectSymbolReplacer(rootExecutionPlan, entry.getValue());
                dependencyFutures.add(consumer.resultFuture().thenAccept(replacer::onSuccess));

                CompletableFuture<ExecutionPlan> planFuture = process(executionPlan, context);
                planFuture.whenComplete((p, e) -> {
                    if (e == null) {
                        // must use plan2TaskVisitor instead of calling execute
                        // to avoid triggering MultiPhasePlans inside p again (they're already processed).
                        // since plan's are not mutated to remove them they're still part of the plan tree
                        plan2TaskVisitor.process(p, null).execute(consumer, Row.EMPTY);
                    } else {
                        consumer.accept(null, e);
                    }
                });
            }
            CompletableFuture[] cfs = dependencyFutures.toArray(new CompletableFuture[0]);
            return CompletableFuture.allOf(cfs).thenCompose(x -> process(rootExecutionPlan, context));
        }
    }

    static class BulkNodeOperationTreeGenerator extends ExecutionPlanVisitor<BulkNodeOperationTreeGenerator.Context, Void> {

        List<NodeOperationTree> createNodeOperationTrees(ExecutionPlan executionPlan, String localNodeId) {
            Context context = new Context(localNodeId);
            process(executionPlan, context);
            return context.nodeOperationTrees;
        }

        @Override
        public Void visitUpsert(Upsert node, Context context) {
            for (ExecutionPlan executionPlan : node.nodes()) {
                context.nodeOperationTrees.add(NodeOperationTreeGenerator.fromPlan(executionPlan, context.localNodeId));
            }
            return null;
        }

        @Override
        protected Void visitPlan(ExecutionPlan executionPlan, Context context) {
            context.nodeOperationTrees.add(NodeOperationTreeGenerator.fromPlan(executionPlan, context.localNodeId));
            return null;
        }

        static class Context {
            private final List<NodeOperationTree> nodeOperationTrees = new ArrayList<>();
            private final String localNodeId;

            public Context(String localNodeId) {
                this.localNodeId = localNodeId;
            }
        }
    }

}
