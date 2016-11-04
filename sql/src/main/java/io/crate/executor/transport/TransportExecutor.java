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

import com.google.common.util.concurrent.ListenableFuture;
import io.crate.action.job.ContextPreparer;
import io.crate.action.sql.DDLStatementDispatcher;
import io.crate.action.sql.ShowStatementDispatcher;
import io.crate.analyze.EvaluatingNormalizer;
import io.crate.analyze.symbol.SelectSymbol;
import io.crate.core.collections.Row;
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
import io.crate.operation.ImplementationSymbolVisitor;
import io.crate.operation.NodeOperation;
import io.crate.operation.NodeOperationTree;
import io.crate.operation.projectors.ProjectionToProjectorVisitor;
import io.crate.operation.projectors.RowReceiver;
import io.crate.planner.*;
import io.crate.planner.distribution.DistributionType;
import io.crate.planner.distribution.UpstreamPhase;
import io.crate.planner.node.ExecutionPhase;
import io.crate.planner.node.ExecutionPhases;
import io.crate.planner.node.ddl.*;
import io.crate.planner.node.dml.ESDelete;
import io.crate.planner.node.dml.Upsert;
import io.crate.planner.node.dml.UpsertById;
import io.crate.planner.node.dql.*;
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

import javax.annotation.Nullable;
import java.util.*;
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
                             BulkRetryCoordinatorPool bulkRetryCoordinatorPool) {
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
        ImplementationSymbolVisitor globalImplementationSymbolVisitor = new ImplementationSymbolVisitor(functions);
        globalProjectionToProjectionVisitor = new ProjectionToProjectorVisitor(
            clusterService,
            functions,
            indexNameExpressionResolver,
            threadPool,
            settings,
            transportActionProvider,
            bulkRetryCoordinatorPool,
            globalImplementationSymbolVisitor,
            normalizer);
    }

    @Override
    public void execute(Plan plan, RowReceiver rowReceiver, Row parameters) {
        CompletableFuture<Plan> planFuture = multiPhaseExecutor.process(plan, null);
        planFuture
            .thenAccept(p -> plan2TaskVisitor.process(p, null).execute(rowReceiver, parameters))
            .exceptionally(t -> { rowReceiver.fail(t); return null; });
    }

    @Override
    public ListenableFuture<List<Long>> executeBulk(Plan plan) {
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
                plan, clusterService.localNode().id());
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
                transportActionProvider.transportShardUpsertActionDelegate(),
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
                SingleRowSingleValueRowReceiver singleRowSingleValueRowReceiver =
                    new SingleRowSingleValueRowReceiver(rootPlan, entry.getValue());

                CompletableFuture<Plan> planFuture = process(plan, context);
                planFuture.whenComplete((p, e) -> {
                    if (e == null) {
                        // must use plan2TaskVisitor instead of calling execute
                        // to avoid triggering MultiPhasePlans inside p again (they're already processed).
                        // since plan's are not mutated to remove them they're still part of the plan tree
                        plan2TaskVisitor.process(p, null).execute(singleRowSingleValueRowReceiver, Row.EMPTY);
                    } else {
                        singleRowSingleValueRowReceiver.fail(e);
                    }
                });
                dependencyFutures.add(singleRowSingleValueRowReceiver.completionFuture());
            }
            CompletableFuture[] cfs = dependencyFutures.toArray(new CompletableFuture[0]);
            return CompletableFuture.allOf(cfs).thenCompose(x -> process(rootPlan, context));
        }
    }

    static class BulkNodeOperationTreeGenerator extends PlanVisitor<BulkNodeOperationTreeGenerator.Context, Void> {

        NodeOperationTreeGenerator nodeOperationTreeGenerator = new NodeOperationTreeGenerator();

        public List<NodeOperationTree> createNodeOperationTrees(Plan plan, String localNodeId) {
            Context context = new Context(localNodeId);
            process(plan, context);
            return context.nodeOperationTrees;
        }

        @Override
        public Void visitUpsert(Upsert node, Context context) {
            for (Plan plan : node.nodes()) {
                context.nodeOperationTrees.add(nodeOperationTreeGenerator.fromPlan(plan, context.localNodeId));
            }
            return null;
        }

        @Override
        protected Void visitPlan(Plan plan, Context context) {
            context.nodeOperationTrees.add(nodeOperationTreeGenerator.fromPlan(plan, context.localNodeId));
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

    /**
     * class used to generate the NodeOperationTree
     * <p>
     * <p>
     * E.g. a plan like NL:
     *
     * <pre>
     *              NL
     *           1 NLPhase
     *           2 MergePhase
     *        /               \
     *       /                 \
     *     QAF                 QAF
     *   3 CollectPhase      5 CollectPhase
     *   4 MergePhase        6 MergePhase
     * </pre>
     *
     * Will have a data flow like this:
     *
     * <pre>
     *   3 -- 4
     *          -- 1 -- 2
     *   5 -- 6
     * </pre>
     * The NodeOperation tree will have 5 NodeOperations (3-4, 4-1, 5-6, 6-1, 1-2)
     * And leaf will be 2 (the Phase which will provide the final result)
     * <p>
     * <p>
     * Implementation detail:
     * <p>
     * <p>
     * The phases are added in the following order
     * <p>
     * 2 - 1 [new branch 0]  4 - 3
     * [new branch 1]  5 - 6
     * <p>
     * every time addPhase is called a NodeOperation is added
     * that connects the previous phase (if there is one) to the current phase
     */
    static class NodeOperationTreeGenerator extends PlanVisitor<NodeOperationTreeGenerator.NodeOperationTreeContext, Void> {

        private static class Branch {
            private final Stack<ExecutionPhase> phases = new Stack<>();
            private final byte inputId;

            public Branch(byte inputId) {
                this.inputId = inputId;
            }
        }

        static class NodeOperationTreeContext {
            private final String localNodeId;
            private final List<NodeOperation> nodeOperations = new ArrayList<>();

            private final Stack<Branch> branches = new Stack<>();
            private final Branch root;
            private Branch currentBranch;

            public NodeOperationTreeContext(String localNodeId) {
                this.localNodeId = localNodeId;
                root = new Branch((byte) 0);
                currentBranch = root;
            }

            /**
             * adds a Phase to the "NodeOperation execution tree"
             * should be called in the reverse order of how data flows.
             * <p>
             * E.g. in a plan where data flows from CollectPhase to MergePhase
             * it should be called first for MergePhase and then for CollectPhase
             */
            public void addPhase(@Nullable ExecutionPhase executionPhase) {
                addPhase(executionPhase, nodeOperations, true);
            }

            public void addContextPhase(@Nullable ExecutionPhase executionPhase) {
                addPhase(executionPhase, nodeOperations, false);
            }

            private void addPhase(@Nullable ExecutionPhase executionPhase,
                                  List<NodeOperation> nodeOperations,
                                  boolean setDownstreamNodes) {
                if (executionPhase == null) {
                    return;
                }
                if (branches.size() == 0 && currentBranch.phases.isEmpty()) {
                    currentBranch.phases.add(executionPhase);
                    return;
                }

                byte inputId;
                ExecutionPhase previousPhase;
                if (currentBranch.phases.isEmpty()) {
                    previousPhase = branches.peek().phases.lastElement();
                    inputId = currentBranch.inputId;
                } else {
                    previousPhase = currentBranch.phases.lastElement();
                    // same branch, so use the default input id
                    inputId = 0;
                }
                if (setDownstreamNodes) {
                    assert saneConfiguration(executionPhase, previousPhase.nodeIds()) : String.format(Locale.ENGLISH,
                        "NodeOperation with %s and %s as downstreams cannot work",
                        ExecutionPhases.debugPrint(executionPhase), previousPhase.nodeIds());

                    nodeOperations.add(NodeOperation.withDownstream(executionPhase, previousPhase, inputId, localNodeId));
                } else {
                    nodeOperations.add(NodeOperation.withoutDownstream(executionPhase));
                }
                currentBranch.phases.add(executionPhase);
            }

            private boolean saneConfiguration(ExecutionPhase executionPhase, Collection<String> downstreamNodes) {
                if (executionPhase instanceof UpstreamPhase &&
                    ((UpstreamPhase) executionPhase).distributionInfo().distributionType() ==
                    DistributionType.SAME_NODE) {
                    return downstreamNodes.isEmpty() || downstreamNodes.equals(executionPhase.nodeIds());
                }
                return true;
            }

            public void branch(byte inputId) {
                branches.add(currentBranch);
                currentBranch = new Branch(inputId);
            }

            public void leaveBranch() {
                currentBranch = branches.pop();
            }


            public Collection<NodeOperation> nodeOperations() {
                return nodeOperations;
            }
        }

        public NodeOperationTree fromPlan(Plan plan, String localNodeId) {
            NodeOperationTreeContext nodeOperationTreeContext = new NodeOperationTreeContext(localNodeId);
            process(plan, nodeOperationTreeContext);
            return new NodeOperationTree(nodeOperationTreeContext.nodeOperations(),
                nodeOperationTreeContext.root.phases.firstElement());
        }

        @Override
        public Void visitDistributedGroupBy(DistributedGroupBy node, NodeOperationTreeContext context) {
            context.addPhase(node.reducerMergeNode());
            context.addPhase(node.collectPhase());
            return null;
        }

        @Override
        public Void visitCountPlan(CountPlan plan, NodeOperationTreeContext context) {
            context.addPhase(plan.mergeNode());
            context.addPhase(plan.countNode());
            return null;
        }

        @Override
        public Void visitCollect(Collect plan, NodeOperationTreeContext context) {
            context.addPhase(plan.collectPhase());
            return null;
        }

        @Override
        public Void visitMerge(Merge merge, NodeOperationTreeContext context) {
            context.addPhase(merge.mergePhase());
            process(merge.subPlan(), context);
            return null;
        }

        public Void visitQueryThenFetch(QueryThenFetch node, NodeOperationTreeContext context) {
            process(node.subPlan(), context);
            context.addContextPhase(node.fetchPhase());
            return null;
        }

        @Override
        public Void visitMultiPhasePlan(MultiPhasePlan multiPhasePlan, NodeOperationTreeContext context) {
            // MultiPhasePlan's should be executed by the MultiPhaseExecutor, but it doesn't remove
            // them from the tree in order to avoid re-creating plans with the MultiPhasePlan removed,
            // so here it's fine to just skip over the multiPhasePlan because it has already been executed
            process(multiPhasePlan.rootPlan(), context);
            return null;
        }

        @Override
        public Void visitNestedLoop(NestedLoop plan, NodeOperationTreeContext context) {
            context.addPhase(plan.nestedLoopPhase());

            context.branch((byte) 0);
            process(plan.left(), context);
            context.leaveBranch();

            context.branch((byte) 1);
            process(plan.right(), context);
            context.leaveBranch();

            return null;
        }

        @Override
        public Void visitUnion(UnionPlan plan, NodeOperationTreeContext context) {
            context.addPhase(plan.unionPhase());

            byte inputId = 0;
            for (Plan subPlan : plan.relations()) {
                context.branch(inputId);
                process(subPlan, context);
                context.leaveBranch();
                inputId++;
            }

            return null;
        }

        @Override
        protected Void visitPlan(Plan plan, NodeOperationTreeContext context) {
            throw new UnsupportedOperationException(String.format(Locale.ENGLISH, "Can't create NodeOperationTree from plan %s", plan));
        }
    }
}
