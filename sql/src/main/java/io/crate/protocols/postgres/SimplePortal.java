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
import io.crate.action.sql.ResultReceiver;
import io.crate.action.sql.RowReceiverToResultReceiver;
import io.crate.action.sql.SessionCtx;
import io.crate.analyze.Analysis;
import io.crate.analyze.Analyzer;
import io.crate.analyze.ParameterContext;
import io.crate.analyze.symbol.Field;
import io.crate.analyze.symbol.Symbols;
import io.crate.concurrent.CompletionListener;
import io.crate.core.collections.Row;
import io.crate.exceptions.Exceptions;
import io.crate.exceptions.ReadOnlyException;
import io.crate.executor.Executor;
import io.crate.executor.transport.kill.KillJobsRequest;
import io.crate.executor.transport.kill.KillResponse;
import io.crate.executor.transport.kill.TransportKillJobsNodeAction;
import io.crate.operation.collect.StatsTables;
import io.crate.operation.projectors.ResumeHandle;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.sql.tree.Statement;
import io.crate.types.DataType;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static io.crate.action.sql.SQLBulkRequest.EMPTY_BULK_ARGS;

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
    private RowReceiverToResultReceiver rowReceiver = null;
    private int maxRows = 0;

    public SimplePortal(String name,
                        SessionCtx sessionCtx,
                        Analyzer analyzer,
                        Executor executor,
                        TransportKillJobsNodeAction transportKillJobsNodeAction,
                        boolean isReadOnly) {
        super(name, analyzer, executor, transportKillJobsNodeAction, isReadOnly, sessionCtx);
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
    public Portal bind(String statementName, String query, Statement statement,
                       List<Object> params, @Nullable FormatCodes.FormatCode[] resultFormatCodes) {

        if (statement.equals(this.statement)) {
            if (sessionData.isReadOnly()) { // Cannot have a bulk operation in read only mode
                throw new ReadOnlyException();
            }
            BulkPortal portal = new BulkPortal(name, this.query, this.statement, outputTypes,
                resultReceiver, maxRows, this.params, sessionCtx, sessionData);
            return portal.bind(statementName, query, statement, params, resultFormatCodes);
        } else if (this.statement != null) {
            if (sessionData.isReadOnly()) { // Cannot have a batch operation in read only mode
                throw new ReadOnlyException();
            }
            BatchPortal portal = new BatchPortal(name, this.query, analysis, outputTypes, resultReceiver,
                this.params, sessionData, sessionCtx);
            return portal.bind(statementName, query, statement, params, resultFormatCodes);
        }

        this.query = query;
        this.statement = statement;
        this.params = params;
        this.resultFormatCodes = resultFormatCodes;
        if (analysis == null) {
            analysis = sessionData.getAnalyzer().analyze(
                statement, new ParameterContext(getArgs(), EMPTY_BULK_ARGS), sessionCtx);
        }
        return this;
    }

    @Override
    public List<Field> describe() {
        if (analysis.rootRelation() == null) {
            return null;
        }
        List<Field> fields = analysis.rootRelation().fields();
        this.outputTypes = Symbols.extractTypes(fields);
        return fields;
    }

    @Override
    public void execute(ResultReceiver resultReceiver, int maxRows) {
        validateReadOnly(analysis);
        this.resultReceiver = resultReceiver;
        this.maxRows = maxRows;
    }

    @Override
    public void sync(Planner planner, StatsTables statsTables, CompletionListener listener) {
        UUID jobId = UUID.randomUUID();
        Plan plan;
        try {
            plan = planner.plan(analysis, jobId, sessionCtx.defaultLimit(), maxRows);
        } catch (Throwable t) {
            statsTables.logPreExecutionFailure(jobId, query, Exceptions.messageOf(t));
            throw t;
        }
        resultReceiver.addListener(listener);
        statsTables.logExecutionStart(jobId, query);
        resultReceiver.addListener(new StatsTablesUpdateListener(jobId, statsTables));

        if (!analysis.analyzedStatement().isWriteOperation()) {
            resultReceiver = new ResultReceiverRetryWrapper(
                resultReceiver,
                this,
                sessionData.getAnalyzer(),
                planner,
                sessionData.getExecutor(),
                sessionData.getTransportKillJobsNodeAction(),
                jobId,
                sessionCtx);
        }

        if (resumeIfSuspended()) return;
        this.rowReceiver = new RowReceiverToResultReceiver(resultReceiver, maxRows);
        sessionData.getExecutor().execute(plan, rowReceiver);
    }

    @Override
    public void close() {
        if (rowReceiver != null) {
            ResumeHandle resumeHandle = rowReceiver.resumeHandle();
            if (resumeHandle != null) {
                rowReceiver.kill(new InterruptedException("Client closed portal"));
                resumeHandle.resume(false);
            }
        }
    }

    private boolean resumeIfSuspended() {
        LOGGER.trace("method=resumeIfSuspended");
        if (rowReceiver == null) {
            return false;
        }
        ResumeHandle resumeHandle = rowReceiver.resumeHandle();
        if (resumeHandle == null) {
            return false;
        }
        rowReceiver.replaceResultReceiver(resultReceiver, maxRows);
        LOGGER.trace("Resuming {}", resumeHandle);
        resumeHandle.resume(true);
        return true;
    }

    private Object[] getArgs() {
        return params.toArray(new Object[0]);
    }

    private void validateReadOnly(Analysis analysis) {
        if (analysis != null && analysis.analyzedStatement().isWriteOperation() && sessionData.isReadOnly()) {
            throw new ReadOnlyException();
        }
    }

    private static class ResultReceiverRetryWrapper implements ResultReceiver {

        private final ResultReceiver delegate;
        private final SimplePortal portal;
        private final Analyzer analyzer;
        private final Planner planner;
        private final Executor executor;
        private final TransportKillJobsNodeAction transportKillJobsNodeAction;
        private final UUID jobId;
        private final SessionCtx sessionCtx;
        int attempt = 1;

        ResultReceiverRetryWrapper(ResultReceiver delegate,
                                   SimplePortal portal,
                                   Analyzer analyzer,
                                   Planner planner,
                                   Executor executor,
                                   TransportKillJobsNodeAction transportKillJobsNodeAction,
                                   UUID jobId,
                                   SessionCtx sessionCtx) {
            this.delegate = delegate;
            this.portal = portal;
            this.analyzer = analyzer;
            this.planner = planner;
            this.executor = executor;
            this.transportKillJobsNodeAction = transportKillJobsNodeAction;
            this.jobId = jobId;
            this.sessionCtx = sessionCtx;
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
        public void allFinished() {
            delegate.allFinished();
        }

        @Override
        public void fail(@Nonnull Throwable t) {
            if (attempt <= Constants.MAX_SHARD_MISSING_RETRIES && Exceptions.isShardFailure(t)) {
                attempt += 1;
                killAndRetry();
            } else {
                delegate.fail(t);
            }
        }

        private void killAndRetry() {
            transportKillJobsNodeAction.executeKillOnAllNodes(
                new KillJobsRequest(Collections.singletonList(jobId)), new ActionListener<KillResponse>() {
                    @Override
                    public void onResponse(KillResponse killResponse) {
                        LOGGER.debug("Killed {} jobs before Retry", killResponse.numKilled());

                        Analysis analysis = analyzer.analyze(portal.statement,
                            new ParameterContext(portal.getArgs(), EMPTY_BULK_ARGS), sessionCtx);
                        Plan plan = planner.plan(analysis, jobId, 0, portal.maxRows);
                        executor.execute(plan, portal.rowReceiver);
                    }

                    @Override
                    public void onFailure(Throwable e) {
                        LOGGER.warn("Failed to kill job before Retry", e);
                        delegate.fail(e);
                    }
                }
            );

        }

        @Override
        public void addListener(CompletionListener listener) {
            throw new UnsupportedOperationException("not supported, listener should be registered already");
        }
    }
}
