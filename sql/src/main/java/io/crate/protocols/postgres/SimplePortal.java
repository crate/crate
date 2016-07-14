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
import io.crate.action.sql.SQLOperations;
import io.crate.analyze.Analysis;
import io.crate.analyze.Analyzer;
import io.crate.analyze.ParameterContext;
import io.crate.concurrent.CompletionListener;
import io.crate.core.collections.Row;
import io.crate.exceptions.Exceptions;
import io.crate.executor.transport.kill.KillJobsRequest;
import io.crate.executor.transport.kill.KillResponse;
import io.crate.operation.collect.StatsTables;
import io.crate.operation.projectors.ResumeHandle;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.planner.statement.SetSessionPlan;
import io.crate.sql.tree.Statement;
import io.crate.types.DataType;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static io.crate.action.sql.SQLBulkRequest.EMPTY_BULK_ARGS;

public class SimplePortal implements Portal {

    private final static ESLogger LOGGER = Loggers.getLogger(SimplePortal.class);

    @Nullable
    private FormatCodes.FormatCode[] resultFormatCodes;
    private List<? extends DataType> outputTypes;

    String name;
    SQLOperations.Session session;
    String query;
    Statement statement;
    Analysis analysis;
    ResultReceiver resultReceiver;
    int maxRows = 0;
    RowReceiverToResultReceiver rowReceiver = null;
    final List<Object> params = new ArrayList<>();

    public SimplePortal(String name, SQLOperations.Session session) {
        this.name = name;
        this.session = session;
    }

    @Override
    public PortalType type() {
        return PortalType.SIMPLE;
    }

    @Override
    public Analysis getAnalysis(String statementName) {
        return analysis;
    }

    @Override
    public Analysis getLastAnalysis() {
        return analysis;
    }

    @Override
    public Object[] getArgs() {
        return params.toArray(new Object[0]);
    }

    @Override
    @Nullable
    public FormatCodes.FormatCode[] getResultFormatCodes() {
        return resultFormatCodes;
    }

    @Override
    public List<? extends DataType> getOutputTypes() {
        return outputTypes;
    }

    @Override
    public Statement getStatement(String statementName) {
        return statement;
    }

    @Override
    public String getLastQuery() {
        return this.query;
    }

    @Override
    public void addQuery(String statementName, String query) {
        this.query = query;
    }

    @Override
    public void addStatement(String statementName, Statement statement) {
        this.statement = statement;
    }

    @Override
    public void addAnalysis(String statementName, Analysis analysis) {
        this.analysis = analysis;
    }

    @Override
    public void addParams(List<Object> params) {
        this.params.addAll(params);
    }

    @Override
    public void addResultFormatCodes(@Nullable FormatCodes.FormatCode[] resultFormatCodes) {
        this.resultFormatCodes = resultFormatCodes;
    }

    @Override
    public void addOutputTypes(List<? extends DataType> outputTypes) {
        this.outputTypes = outputTypes;
    }

    @Override
    public void setMaxRows(int maxRows) {
        this.maxRows = maxRows;
    }

    @Override
    public void addResultReceiver(ResultReceiver resultReceiver) {
        this.resultReceiver = resultReceiver;
    }

    @Override
    public void setRowReceiver(RowReceiverToResultReceiver rowReceiver) {
        this.rowReceiver = rowReceiver;
    }

    @Override
    public void execute(Analyzer analyzer, Planner planner, StatsTables statsTables, CompletionListener listener) {
        Plan plan = planner.plan(analysis, session.jobId(), 0, maxRows);
        applySessionSettings(plan, session);
        resultReceiver.addListener(listener);
        resultReceiver.addListener(new StatsTablesUpdateListener(session.jobId(), statsTables));

        if (!analysis.analyzedStatement().isWriteOperation()) {
            resultReceiver = new ResultReceiverRetryWrapper(resultReceiver, session, this, analyzer, planner);
        }

        if (resumeIfSuspended()) return;
        this.rowReceiver = new RowReceiverToResultReceiver(resultReceiver, maxRows);
        session.getExecutor().execute(plan, rowReceiver);
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

    private void applySessionSettings(Plan plan, SQLOperations.Session session) {
        if (plan instanceof SetSessionPlan) {
            session.addSettings(((SetSessionPlan) plan).settings().getAsMap());
        }
    }

    private static class ResultReceiverRetryWrapper implements ResultReceiver {

        private final ResultReceiver delegate;
        private final SQLOperations.Session session;
        private final SimplePortal portal;
        private final Analyzer analyzer;
        private final Planner planner;
        int attempt = 1;

        ResultReceiverRetryWrapper(ResultReceiver delegate,
                                   SQLOperations.Session session,
                                   SimplePortal portal,
                                   Analyzer analyzer,
                                   Planner planner) {
            this.delegate = delegate;
            this.session = session;
            this.portal = portal;
            this.analyzer = analyzer;
            this.planner = planner;
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
            session.getTransportKillJobsNodeAction().executeKillOnAllNodes(
                new KillJobsRequest(Collections.singletonList(session.jobId())), new ActionListener<KillResponse>() {
                    @Override
                    public void onResponse(KillResponse killResponse) {
                        LOGGER.debug("Killed {} jobs before Retry", killResponse.numKilled());

                        Analysis analysis = analyzer.analyze(portal.statement,
                            new ParameterContext(portal.getArgs(),EMPTY_BULK_ARGS, session.getDefaultSchema()));
                        Plan plan = planner.plan(analysis, session.jobId(), 0, portal.maxRows);
                        session.getExecutor().execute(plan, portal.rowReceiver);
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
