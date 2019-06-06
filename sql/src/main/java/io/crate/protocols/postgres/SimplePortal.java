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

package io.crate.protocols.postgres;

import io.crate.action.sql.ResultReceiver;
import io.crate.action.sql.RowConsumerToResultReceiver;
import io.crate.action.sql.SessionContext;
import io.crate.analyze.Analysis;
import io.crate.analyze.AnalyzedStatement;
import io.crate.analyze.Analyzer;
import io.crate.analyze.ParameterContext;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.common.collections.Lists2;
import io.crate.data.Row;
import io.crate.data.RowN;
import io.crate.exceptions.ReadOnlyException;
import io.crate.exceptions.SQLExceptions;
import io.crate.execution.engine.collect.stats.JobsLogs;
import io.crate.expression.symbol.Field;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.RoutingProvider;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.planner.PlannerContext;
import io.crate.planner.operators.StatementClassifier;
import io.crate.planner.operators.SubQueryResults;
import io.crate.sql.tree.Statement;
import io.crate.types.DataType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.Randomness;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class SimplePortal extends AbstractPortal {

    private static final Logger LOGGER = LogManager.getLogger(SimplePortal.class);

    private List<Object> params;
    private String query;
    private Statement statement;
    @Nullable
    private AnalyzedStatement analyzedStatement;
    @Nullable
    private FormatCodes.FormatCode[] resultFormatCodes;
    private List<? extends DataType> outputTypes;
    private ResultReceiver<?> resultReceiver;
    private RowConsumerToResultReceiver consumer = null;
    private int maxRows = 0;
    private Row rowParams;
    private CoordinatorTxnCtx coordinatorTxnCtx;

    public SimplePortal(String name,
                        Analyzer analyzer,
                        DependencyCarrier executor,
                        boolean isReadOnly,
                        SessionContext sessionContext) {
        super(name, analyzer, executor, isReadOnly, sessionContext);
    }

    @Override
    @Nullable
    public FormatCodes.FormatCode[] getLastResultFormatCodes() {
        return resultFormatCodes;
    }

    @Override
    public List<? extends DataType> getLastOutputTypes() {
        return outputTypes;
    }

    @Override
    public String getLastQuery() {
        return query;
    }

    @Override
    public AnalyzedStatement getLastAnalyzedStatement() {
        return analyzedStatement;
    }

    @Override
    public Portal bind(String statementName,
                       String query,
                       Statement statement,
                       @Nullable AnalyzedStatement analyzedStatement,
                       List<Object> params,
                       @Nullable FormatCodes.FormatCode[] resultFormatCodes) {

        if (statement.equals(this.statement)) {
            if (portalContext.isReadOnly()) { // Cannot have a bulk operation in read only mode
                throw new ReadOnlyException();
            }
            assert consumer == null : "Existing portal must not have a consumer";
            BulkPortal portal = new BulkPortal(
                name,
                this.query,
                this.statement,
                outputTypes,
                fields(),
                resultReceiver, maxRows, this.params, sessionContext, portalContext);
            return portal.bind(statementName, query, statement, null, params, resultFormatCodes);
        } else if (this.statement != null && this.analyzedStatement != null) {
            assert consumer == null : "Existing portal must not have a consumer";
            if (portalContext.isReadOnly()) { // Cannot have a batch operation in read only mode
                throw new ReadOnlyException();
            }
            BatchPortal portal = new BatchPortal(
                name, this.query, this.analyzedStatement, outputTypes, resultReceiver, this.params, sessionContext, portalContext);
            return portal.bind(statementName, query, statement, analyzedStatement, params, resultFormatCodes);
        }

        this.query = query;
        this.statement = statement;
        this.params = params;
        this.rowParams = new RowN(params.toArray());
        this.resultFormatCodes = resultFormatCodes;
        if (coordinatorTxnCtx == null) {
            coordinatorTxnCtx = new CoordinatorTxnCtx(sessionContext);
        }
        if (analyzedStatement == null || analyzedStatement.isUnboundPlanningSupported() == false) {
            Analysis analysis = portalContext.getAnalyzer().boundAnalyze(
                statement,
                coordinatorTxnCtx,
                new ParameterContext(rowParams, Collections.emptyList()));
            analyzedStatement = analysis.analyzedStatement();
        }
        if (analyzedStatement instanceof AnalyzedRelation) {
            AnalyzedRelation rootRelation = (AnalyzedRelation) analyzedStatement;
            this.outputTypes = Lists2.map(rootRelation.fields(), Field::valueType);
        }

        this.analyzedStatement = analyzedStatement;
        return this;
    }

    @Override
    public List<Field> describe() {
        return fields();
    }

    @Override
    public void execute(ResultReceiver resultReceiver, int maxRows) {
        validateReadOnly(analyzedStatement);
        this.resultReceiver = resultReceiver;
        this.maxRows = maxRows;
    }

    @Override
    public CompletableFuture<?> sync(Planner planner, JobsLogs jobsLogs) {
        assert analyzedStatement != null : "analyzedStatement must not be null";

        if (consumer != null && consumer.suspended()) {
            LOGGER.trace("Resuming existing consumer {}", consumer);
            consumer.replaceResultReceiver(resultReceiver, maxRows);
            consumer.resume();
            return resultReceiver.completionFuture();
        }

        UUID jobId = UUID.randomUUID();
        RoutingProvider routingProvider = new RoutingProvider(Randomness.get().nextInt(), planner.getAwarenessAttributes());
        ClusterState clusterState = planner.currentClusterState();
        PlannerContext plannerContext = new PlannerContext(
            clusterState,
            routingProvider,
            jobId,
            planner.functions(),
            coordinatorTxnCtx,
            maxRows
        );
        Plan plan;
        try {
            plan = planner.plan(analyzedStatement, plannerContext);
        } catch (Throwable t) {
            jobsLogs.logPreExecutionFailure(jobId, query, SQLExceptions.messageOf(t), sessionContext.user());
            throw t;
        }

        DependencyCarrier dependencyCarrier = portalContext.getExecutor();
        if (!analyzedStatement.isWriteOperation()) {
            resultReceiver = new RetryOnFailureResultReceiver(
                dependencyCarrier.clusterService(),
                clusterState,
                dependencyCarrier.threadPool().getThreadContext(),
                // not using planner.currentClusterState().metaData()::hasIndex to make sure the *current*
                // clusterState at the time of the index check is used
                indexName -> clusterState.metaData().hasIndex(indexName),
                resultReceiver,
                jobId,
                (newJobId, resultReceiver) -> retryQuery(planner, newJobId)
            );
        }

        jobsLogs.logExecutionStart(jobId, query, sessionContext.user(), StatementClassifier.classify(plan));
        consumer = new RowConsumerToResultReceiver(
            resultReceiver,
            maxRows,
            new JobsLogsUpdateListener(jobId, jobsLogs)
        );
        plan.execute(
            dependencyCarrier,
            plannerContext,
            consumer,
            rowParams,
            SubQueryResults.EMPTY
        );
        synced = true;
        return resultReceiver.completionFuture();
    }

    private void retryQuery(Planner planner, UUID jobId) {
        if (consumer == null) {
            return; // portal was closed
        }
        Analysis analysis = portalContext
            .getAnalyzer()
            .boundAnalyze(statement, coordinatorTxnCtx, new ParameterContext(rowParams, Collections.emptyList()));
        RoutingProvider routingProvider = new RoutingProvider(Randomness.get().nextInt(), planner.getAwarenessAttributes());
        PlannerContext plannerContext = new PlannerContext(
            planner.currentClusterState(),
            routingProvider,
            jobId,
            planner.functions(),
            coordinatorTxnCtx,
            maxRows
        );
        Plan plan = planner.plan(analysis.analyzedStatement(), plannerContext);
        plan.execute(
            portalContext.getExecutor(),
            plannerContext,
            consumer,
            rowParams,
            SubQueryResults.EMPTY
        );
    }

    @Override
    public void close() {
        if (consumer != null) {
            consumer.closeAndFinishIfSuspended();
            consumer = null;
        }
    }

    private void validateReadOnly(AnalyzedStatement analyzedStatement) {
        if (analyzedStatement != null && analyzedStatement.isWriteOperation() && portalContext.isReadOnly()) {
            throw new ReadOnlyException();
        }
    }

    private List<Field> fields() {
        if (analyzedStatement == null || analyzedStatement instanceof AnalyzedRelation == false) {
            return null;
        }
        return ((AnalyzedRelation) analyzedStatement).fields();
    }

}
