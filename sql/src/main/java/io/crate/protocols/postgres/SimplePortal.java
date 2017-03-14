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

import io.crate.Constants;
import io.crate.action.sql.BatchConsumerToResultReceiver;
import io.crate.action.sql.ResultReceiver;
import io.crate.action.sql.SessionContext;
import io.crate.analyze.Analysis;
import io.crate.analyze.Analyzer;
import io.crate.analyze.ParameterContext;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.symbol.Field;
import io.crate.analyze.symbol.Symbols;
import io.crate.data.Row;
import io.crate.data.RowN;
import io.crate.exceptions.ReadOnlyException;
import io.crate.exceptions.SQLExceptions;
import io.crate.executor.Executor;
import io.crate.operation.collect.stats.JobsLogs;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.sql.tree.Statement;
import io.crate.types.DataType;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class SimplePortal extends AbstractPortal {

    private final static ESLogger LOGGER = Loggers.getLogger(SimplePortal.class);

    private List<Object> params;
    private String query;
    private Statement statement;
    private Analysis analysis;
    @Nullable
    private FormatCodes.FormatCode[] resultFormatCodes;
    private List<? extends DataType> outputTypes;
    private ResultReceiver resultReceiver;
    private BatchConsumerToResultReceiver consumer = null;
    private int maxRows = 0;
    private int defaultLimit;
    private Row rowParams;

    public SimplePortal(String name,
                        Analyzer analyzer,
                        Executor executor,
                        boolean isReadOnly,
                        SessionContext sessionContext) {
        super(name, analyzer, executor, isReadOnly, sessionContext);
        this.defaultLimit = sessionContext.defaultLimit();
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
        return this.query;
    }

    @Override
    public Portal bind(String statementName,
                       String query,
                       Statement statement,
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
            return portal.bind(statementName, query, statement, params, resultFormatCodes);
        } else if (this.statement != null) {
            assert consumer == null : "Existing portal must not have a consumer";
            if (portalContext.isReadOnly()) { // Cannot have a batch operation in read only mode
                throw new ReadOnlyException();
            }
            BatchPortal portal = new BatchPortal(
                name, this.query, analysis, outputTypes, resultReceiver, this.params, sessionContext, portalContext);
            return portal.bind(statementName, query, statement, params, resultFormatCodes);
        }

        this.query = query;
        this.statement = statement;
        this.params = params;
        this.rowParams = new RowN(params.toArray());
        this.resultFormatCodes = resultFormatCodes;
        if (analysis == null) {
            analysis = portalContext.getAnalyzer().boundAnalyze(
                statement,
                sessionContext,
                new ParameterContext(this.rowParams, Collections.<Row>emptyList()));
            AnalyzedRelation rootRelation = analysis.rootRelation();
            if (rootRelation != null) {
                this.outputTypes = new ArrayList<>(Symbols.extractTypes(rootRelation.fields()));
            }
        }
        return this;
    }

    @Override
    public List<Field> describe() {
        return fields();
    }

    @Override
    public void execute(ResultReceiver resultReceiver, int maxRows) {
        validateReadOnly(analysis);
        this.resultReceiver = resultReceiver;
        this.maxRows = maxRows;
    }

    @Override
    public CompletableFuture<?> sync(Planner planner, JobsLogs jobsLogs) {
        UUID jobId = UUID.randomUUID();
        Plan plan;
        try {
            plan = planner.plan(analysis, jobId, defaultLimit, maxRows);
        } catch (Throwable t) {
            jobsLogs.logPreExecutionFailure(jobId, query, SQLExceptions.messageOf(t));
            throw t;
        }

        if (!analysis.analyzedStatement().isWriteOperation()) {
            resultReceiver = new ResultReceiverRetryWrapper(
                resultReceiver,
                this,
                portalContext.getAnalyzer(),
                planner,
                portalContext.getExecutor(),
                jobId,
                sessionContext);
        }

        jobsLogs.logExecutionStart(jobId, query);
        JobsLogsUpdateListener jobsLogsUpdateListener = new JobsLogsUpdateListener(jobId, jobsLogs);
        CompletableFuture completableFuture = resultReceiver.completionFuture().whenComplete(jobsLogsUpdateListener);

        if (!resumeIfSuspended()) {
            consumer = new BatchConsumerToResultReceiver(resultReceiver, maxRows);
            portalContext.getExecutor().execute(plan, consumer, this.rowParams);
        }
        synced = true;
        return completableFuture;
    }

    @Override
    public void close() {
        if (consumer != null) {
            consumer.interruptIfResumable();
        }
    }

    private boolean resumeIfSuspended() {
        LOGGER.trace("method=resumeIfSuspended");
        if (consumer == null) {
            return false;
        }
        if (consumer.suspended()) {
            consumer.replaceResultReceiver(resultReceiver, maxRows);
            LOGGER.trace("Resuming {}", consumer);
            consumer.resume();
            return true;
        } else {
            return false;
        }
    }

    private void validateReadOnly(Analysis analysis) {
        if (analysis != null && analysis.analyzedStatement().isWriteOperation() && portalContext.isReadOnly()) {
            throw new ReadOnlyException();
        }
    }

    private List<Field> fields() {
        if (analysis.rootRelation() == null) {
            return null;
        }
        return analysis.rootRelation().fields();
    }

    private static class ResultReceiverRetryWrapper implements ResultReceiver {

        private final ResultReceiver delegate;
        private final SimplePortal portal;
        private final Analyzer analyzer;
        private final Planner planner;
        private final Executor executor;
        private final UUID jobId;
        private final SessionContext sessionContext;
        int attempt = 1;

        ResultReceiverRetryWrapper(ResultReceiver delegate,
                                   SimplePortal portal,
                                   Analyzer analyzer,
                                   Planner planner,
                                   Executor executor,
                                   UUID jobId,
                                   SessionContext sessionContext) {
            this.delegate = delegate;
            this.portal = portal;
            this.analyzer = analyzer;
            this.planner = planner;
            this.executor = executor;
            this.jobId = jobId;
            this.sessionContext = sessionContext;
        }

        @Override
        public void setNextRow(Row row) {
            delegate.setNextRow(row);
        }

        @Override
        public void batchFinished() {
            delegate.batchFinished();
        }

        @Override
        public void allFinished(boolean interrupted) {
            delegate.allFinished(interrupted);
        }

        @Override
        public void fail(@Nonnull Throwable t) {
            if (attempt <= Constants.MAX_SHARD_MISSING_RETRIES && SQLExceptions.isShardFailure(t)) {
                attempt += 1;
                retry();
            } else {
                delegate.fail(t);
            }
        }

        private void retry() {
            UUID newJobId = UUID.randomUUID();
            LOGGER.debug("Retrying statement due to a shard failure, attempt={}, jobId={}->{}", attempt, jobId, newJobId);
            Analysis analysis = analyzer.boundAnalyze(portal.statement, sessionContext,
                new ParameterContext(portal.rowParams, Collections.<Row>emptyList()));

            Plan plan = planner.plan(analysis, newJobId, 0, portal.maxRows);
            executor.execute(plan, portal.consumer, portal.rowParams);
        }

        @Override
        public CompletableFuture<?> completionFuture() {
            return delegate.completionFuture();
        }
    }
}
