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
import io.crate.action.sql.SessionContext;
import io.crate.analyze.Analysis;
import io.crate.analyze.AnalyzedStatement;
import io.crate.analyze.ParameterContext;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowN;
import io.crate.data.Rows;
import io.crate.exceptions.SQLExceptions;
import io.crate.execution.engine.collect.stats.JobsLogs;
import io.crate.expression.symbol.Field;
import io.crate.metadata.RoutingProvider;
import io.crate.metadata.TransactionContext;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.planner.PlannerContext;
import io.crate.sql.tree.Statement;
import io.crate.types.DataType;
import org.elasticsearch.common.Randomness;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

class BulkPortal extends AbstractPortal {

    private static final Runnable NO_OP_ACTION = () -> { };

    private final List<List<Object>> bulkArgs = new ArrayList<>();
    private final List<ResultReceiver> resultReceivers = new ArrayList<>();
    private String query;
    private Statement statement;
    @Nullable
    private AnalyzedStatement analyzedStatement;
    private int maxRows = 0;
    private List<? extends DataType> outputTypes;
    private List<Field> fields;

    BulkPortal(String name,
               String query,
               Statement statement,
               @Nullable AnalyzedStatement analyzedStatement,
               List<? extends DataType> outputTypes,
               @Nullable List<Field> fields,
               ResultReceiver resultReceiver,
               int maxRows,
               List<Object> params,
               SessionContext sessionContext,
               PortalContext portalContext) {
        super(name, sessionContext, portalContext);
        this.query = query;
        this.statement = statement;
        this.outputTypes = outputTypes;
        this.fields = fields;
        this.resultReceivers.add(resultReceiver);
        this.maxRows = maxRows;
        this.bulkArgs.add(params);
    }

    @Override
    public FormatCodes.FormatCode[] getLastResultFormatCodes() {
        return new FormatCodes.FormatCode[0];
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
        this.bulkArgs.add(params);
        return this;
    }

    @Override
    public List<Field> describe() {
        return fields;
    }

    @Override
    public void execute(ResultReceiver resultReceiver, int maxRows) {
        this.resultReceivers.add(resultReceiver);
        this.maxRows = maxRows;
    }

    @Override
    public CompletableFuture<?> sync(Planner planner, JobsLogs jobsLogs) {
        List<Row> bulkParams = Rows.of(bulkArgs);
        TransactionContext transactionContext = new TransactionContext(sessionContext);

        if (analyzedStatement == null) {
            Analysis analysis = portalContext.getAnalyzer().boundAnalyze(statement,
                transactionContext,
                new ParameterContext(Row.EMPTY, bulkParams));
            analyzedStatement = analysis.analyzedStatement();
        }

        UUID jobId = UUID.randomUUID();
        RoutingProvider routingProvider = new RoutingProvider(Randomness.get().nextInt(), planner.getAwarenessAttributes());
        PlannerContext plannerContext = new PlannerContext(
            planner.currentClusterState(),
            routingProvider,
            jobId,
            planner.functions(),
            transactionContext,
            0,
            maxRows
        );
        Plan plan;
        try {
            plan = planner.plan(analyzedStatement, plannerContext);
        } catch (Throwable t) {
            jobsLogs.logPreExecutionFailure(jobId, query, SQLExceptions.messageOf(t), sessionContext.user());
            throw t;
        }
        jobsLogs.logExecutionStart(jobId, query, sessionContext.user());
        synced = true;
        return executeBulk(portalContext.getExecutor(), plan, plannerContext, jobId, jobsLogs, bulkParams);
    }

    private CompletableFuture<Void> executeBulk(DependencyCarrier executor,
                                                Plan plan,
                                                PlannerContext plannerContext,
                                                final UUID jobId,
                                                final JobsLogs jobsLogs,
                                                List<Row> bulkParams) {
        List<CompletableFuture<Long>> rowCounts = plan.executeBulk(
            executor,
            plannerContext,
            bulkParams,
            Collections.emptyMap()
        );
        CompletableFuture<Void> allFutures = CompletableFuture.allOf(rowCounts.toArray(new CompletableFuture[0]));

        ArrayList<CompletableFuture> resultReceiversCompletionFutures = new ArrayList<>(resultReceivers.size());
        for (ResultReceiver resultReceiver : resultReceivers) {
            resultReceiversCompletionFutures.add(resultReceiver.completionFuture());
        }
        CompletableFuture<Void> allResultReceivers =
            CompletableFuture.allOf(resultReceiversCompletionFutures.toArray(new CompletableFuture[0]));

        // expose a CompletionStage that completes when all the ResultReceivers are complete (not just finished)
        return allFutures
            .exceptionally(t -> null) // swallow exception - failures are set per item in emitResults
            .thenAccept(ignored -> emitResults(jobId, jobsLogs, rowCounts))
            .runAfterBoth(allResultReceivers, NO_OP_ACTION);
    }

    private void emitResults(UUID jobId, JobsLogs jobsLogs, List<CompletableFuture<Long>> completedResultFutures) {
        assert completedResultFutures.size() == resultReceivers.size()
            : "number of result must match number of rowReceivers, results: " +
              "" + completedResultFutures.size() + "; receivers: " + resultReceivers.size();

        Long[] cells = new Long[1];
        RowN row = new RowN(cells);
        for (int i = 0; i < completedResultFutures.size(); i++) {
            CompletableFuture<Long> completedResultFuture = completedResultFutures.get(i);
            ResultReceiver resultReceiver = resultReceivers.get(i);
            try {
                Long rowCount = completedResultFuture.join();
                cells[0] = rowCount == null ? Row1.ERROR : rowCount;
            } catch (Throwable t) {
                cells[0] = Row1.ERROR;
            }
            resultReceiver.setNextRow(row);
            resultReceiver.allFinished(false);
        }
        jobsLogs.logExecutionEnd(jobId, null);
    }
}
