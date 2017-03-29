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
import io.crate.action.sql.DDLStatementDispatcher;
import io.crate.action.sql.ShowStatementDispatcher;
import io.crate.analyze.EvaluatingNormalizer;
import io.crate.analyze.symbol.SelectSymbol;
import io.crate.data.BatchConsumer;
import io.crate.data.CollectingBatchConsumer;
import io.crate.data.Row;
import io.crate.executor.Executor;
import io.crate.executor.Task;
import io.crate.executor.task.DDLTask;
import io.crate.executor.task.ExplainTask;
import io.crate.executor.task.NoopTask;
import io.crate.executor.task.SetSessionTask;
import io.crate.executor.transport.executionphases.ExecutionPhasesTask;
import io.crate.executor.transport.task.*;
import io.crate.executor.transport.task.elasticsearch.*;
import io.crate.jobs.JobContextService;
import io.crate.metadata.Functions;
import io.crate.metadata.ReplaceMode;
import io.crate.operation.InputFactory;
import io.crate.operation.NodeOperationTree;
import io.crate.operation.collect.sources.SystemCollectSource;
import io.crate.operation.projectors.ProjectionToProjectorVisitor;
import io.crate.planner.*;
import io.crate.planner.node.ddl.*;
import io.crate.planner.node.dml.ESDelete;
import io.crate.planner.node.dml.Upsert;
import io.crate.planner.node.dml.UpsertById;
import io.crate.planner.node.dql.ESGet;
import io.crate.planner.node.dql.QueryThenFetch;
import io.crate.planner.node.dql.join.NestedLoop;
import io.crate.planner.node.management.ExplainPlan;
import io.crate.planner.node.management.GenericShowPlan;
import io.crate.planner.node.management.KillPlan;
import io.crate.planner.statement.SetSessionPlan;
import org.elasticsearch.action.bulk.BulkRetryCoordinatorPool;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Singleton
public class TransportExecutor implements Executor {

    private static final ESLogger LOGGER = Loggers.getLogger(TransportExecutor.class);

    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final Functions functions;
    private final TaskCollectingVisitor plan2TaskVisitor;
    private DDLStatementDispatcher ddlAnalysisDispatcherProvider;
    private ShowStatementDispatcher showStatementDispatcherProvider;

    private final ClusterService clusterService;
    private final JobContextService jobContextService;
    private final ContextPreparer contextPreparer;
    private final TransportActionProvider transportActionProvider;
    private final IndicesService indicesService;
    private final BulkRetryCoordinatorPool bulkRetryCoordinatorPool;

    private final ProjectionToProjectorVisitor globalProjectionToProjectionVisitor;
    private final MultiPhaseExecutor multiPhaseExecutor = new MultiPhaseExecutor();

    private final static BulkNodeOperationTreeGenerator BULK_NODE_OPERATION_VISITOR = new BulkNodeOperationTreeGenerator();


    @Inject
    public TransportExecutor(Settings settings,
                             JobContextService jobContextService,
                             ContextPreparer contextPreparer,
                             TransportActionProvider transportActionProvider,
                             IndexNameExpressionResolver indexNameExpressionResolver,
                             ThreadPool threadPool,
                             Functions functions,
                             DDLStatementDispatcher ddlAnalysisDispatcherProvider,
                             ShowStatementDispatcher showStatementDispatcherProvider,
                             ClusterService clusterService,
                             IndicesService indicesService,
                             BulkRetryCoordinatorPool bulkRetryCoordinatorPool,
                             SystemCollectSource systemCollectSource) {
        this.jobContextService = jobContextService;
        this.contextPreparer = contextPreparer;
        this.transportActionProvider = transportActionProvider;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.functions = functions;
        this.ddlAnalysisDispatcherProvider = ddlAnalysisDispatcherProvider;
        this.showStatementDispatcherProvider = showStatementDispatcherProvider;
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.bulkRetryCoordinatorPool = bulkRetryCoordinatorPool;
        plan2TaskVisitor = new TaskCollectingVisitor();
        EvaluatingNormalizer normalizer = EvaluatingNormalizer.functionOnlyNormalizer(functions, ReplaceMode.COPY);
        globalProjectionToProjectionVisitor = new ProjectionToProjectorVisitor(
            clusterService,
            functions,
            indexNameExpressionResolver,
            threadPool,
            settings,
            transportActionProvider,
            bulkRetryCoordinatorPool,
            new InputFactory(functions),
            normalizer,
            systemCollectSource::getRowUpdater
            );
    }

    @Override
    public void execute(Plan plan, BatchConsumer consumer, Row parameters) {
        CompletableFuture<Plan> planFuture = multiPhaseExecutor.process(plan, null);
        planFuture
            .thenAccept(p -> plan2TaskVisitor.process(p, null).execute(consumer, parameters))
            .exceptionally(t -> { consumer.accept(null, t); return null; });
    }

    @Override
    public List<CompletableFuture<Long>> executeBulk(Plan plan) {
        Task task = plan2TaskVisitor.process(plan, null);
        return task.executeBulk();
    }

    private class TaskCollectingVisitor extends PlanVisitor<Void, Task> {

        @Override
        public Task visitNoopPlan(NoopPlan plan, Void context) {
            return NoopTask.INSTANCE;
        }

        @Override
        public Task visitSetSessionPlan(SetSessionPlan plan, Void context) {
            return new SetSessionTask(plan.jobId(), plan.settings(), plan.sessionContext());
        }

        @Override
        public Task visitExplainPlan(ExplainPlan explainPlan, Void context) {
            return new ExplainTask(explainPlan);
        }

        @Override
        protected Task visitPlan(Plan plan, Void context) {
            return executionPhasesTask(plan);
        }

        private ExecutionPhasesTask executionPhasesTask(Plan plan) {
            List<NodeOperationTree> nodeOperationTrees = BULK_NODE_OPERATION_VISITOR.createNodeOperationTrees(
                plan, clusterService.localNode().getId());
            LOGGER.debug("Created NodeOperationTrees from Plan: {}", nodeOperationTrees);
            return new ExecutionPhasesTask(
                plan.jobId(),
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
        public Task visitGetPlan(ESGet plan, Void context) {
            return new ESGetTask(
                functions,
                globalProjectionToProjectionVisitor,
                transportActionProvider,
                plan,
                jobContextService);
        }

        @Override
        public Task visitDropTablePlan(DropTablePlan plan, Void context) {
            return new DropTableTask(plan,
                transportActionProvider.transportDeleteIndexTemplateAction(),
                transportActionProvider.transportDeleteIndexAction());
        }

        @Override
        public Task visitKillPlan(KillPlan killPlan, Void context) {
            return killPlan.jobToKill().isPresent() ?
                new KillJobTask(transportActionProvider.transportKillJobsNodeAction(),
                    killPlan.jobId(),
                    killPlan.jobToKill().get()) :
                new KillTask(transportActionProvider.transportKillAllNodeAction(), killPlan.jobId());
        }

        @Override
        public Task visitGenericShowPlan(GenericShowPlan genericShowPlan, Void context) {
            return new GenericShowTask(genericShowPlan.jobId(), showStatementDispatcherProvider, genericShowPlan.statement());
        }

        @Override
        public Task visitGenericDDLPLan(GenericDDLPlan genericDDLPlan, Void context) {
            return new DDLTask(genericDDLPlan.jobId(), ddlAnalysisDispatcherProvider, genericDDLPlan.statement());
        }

        @Override
        public Task visitESClusterUpdateSettingsPlan(ESClusterUpdateSettingsPlan plan, Void context) {
            return new ESClusterUpdateSettingsTask(plan, transportActionProvider.transportClusterUpdateSettingsAction());
        }

        @Override
        public Task visitCreateAnalyzerPlan(CreateAnalyzerPlan plan, Void context) {
            return new CreateAnalyzerTask(plan, transportActionProvider.transportClusterUpdateSettingsAction());
        }

        @Override
        public Task visitESDelete(ESDelete plan, Void context) {
            return new ESDeleteTask(plan, transportActionProvider.transportDeleteAction(), jobContextService);
        }

        @Override
        public Task visitUpsertById(UpsertById plan, Void context) {
            return new UpsertByIdTask(
                plan,
                clusterService,
                indexNameExpressionResolver,
                clusterService.state().metaData().settings(),
                transportActionProvider.transportShardUpsertAction()::execute,
                transportActionProvider.transportCreateIndexAction(),
                transportActionProvider.transportBulkCreateIndicesAction(),
                bulkRetryCoordinatorPool,
                jobContextService);
        }

        @Override
        public Task visitESDeletePartition(ESDeletePartition plan, Void context) {
            return new ESDeletePartitionTask(plan, transportActionProvider.transportDeleteIndexAction());
        }

        @Override
        public Task visitMultiPhasePlan(MultiPhasePlan multiPhasePlan, final Void context) {
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
    private class MultiPhaseExecutor extends PlanVisitor<Void, CompletableFuture<Plan>> {

        @Override
        protected CompletableFuture<Plan> visitPlan(Plan plan, Void context) {
            return CompletableFuture.completedFuture(plan);
        }

        @Override
        public CompletableFuture<Plan> visitMerge(Merge merge, Void context) {
            return process(merge.subPlan(), context).thenApply(p -> merge);
        }

        @Override
        public CompletableFuture<Plan> visitNestedLoop(NestedLoop plan, Void context) {
            CompletableFuture<Plan> fLeft = process(plan.left(), context);
            CompletableFuture<Plan> fRight = process(plan.right(), context);
            return CompletableFuture.allOf(fLeft, fRight).thenApply(x -> plan);
        }

        @Override
        public CompletableFuture<Plan> visitQueryThenFetch(QueryThenFetch qtf, Void context) {
            return process(qtf.subPlan(), context).thenApply(x -> qtf);
        }

        @Override
        public CompletableFuture<Plan> visitMultiPhasePlan(MultiPhasePlan multiPhasePlan, Void context) {
            Map<Plan, SelectSymbol> dependencies = multiPhasePlan.dependencies();
            List<CompletableFuture<?>> dependencyFutures = new ArrayList<>();
            Plan rootPlan = multiPhasePlan.rootPlan();
            for (Map.Entry<Plan, SelectSymbol> entry : dependencies.entrySet()) {
                Plan plan = entry.getKey();

                SubSelectSymbolReplacer replacer = new SubSelectSymbolReplacer(rootPlan, entry.getValue());
                CollectingBatchConsumer<Object[], Object> consumer = SingleRowSingleValueConsumer.create();

                CompletableFuture<Plan> planFuture = process(plan, context);
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
                dependencyFutures.add(consumer.resultFuture().thenAccept(replacer::onSuccess));
            }
            CompletableFuture[] cfs = dependencyFutures.toArray(new CompletableFuture[0]);
            return CompletableFuture.allOf(cfs).thenCompose(x -> process(rootPlan, context));
        }
    }

    static class BulkNodeOperationTreeGenerator extends PlanVisitor<BulkNodeOperationTreeGenerator.Context, Void> {

        List<NodeOperationTree> createNodeOperationTrees(Plan plan, String localNodeId) {
            Context context = new Context(localNodeId);
            process(plan, context);
            return context.nodeOperationTrees;
        }

        @Override
        public Void visitUpsert(Upsert node, Context context) {
            for (Plan plan : node.nodes()) {
                context.nodeOperationTrees.add(NodeOperationTreeGenerator.fromPlan(plan, context.localNodeId));
            }
            return null;
        }

        @Override
        protected Void visitPlan(Plan plan, Context context) {
            context.nodeOperationTrees.add(NodeOperationTreeGenerator.fromPlan(plan, context.localNodeId));
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
