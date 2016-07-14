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

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.action.sql.ResultReceiver;
import io.crate.action.sql.RowReceiverToResultReceiver;
import io.crate.action.sql.SQLOperations;
import io.crate.analyze.Analysis;
import io.crate.analyze.Analyzer;
import io.crate.analyze.ParameterContext;
import io.crate.concurrent.CompletionListener;
import io.crate.exceptions.Exceptions;
import io.crate.executor.Executor;
import io.crate.executor.TaskResult;
import io.crate.operation.collect.StatsTables;
import io.crate.operation.projectors.ResumeHandle;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.sql.tree.Statement;
import io.crate.types.DataType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static io.crate.action.sql.SQLRequest.EMPTY_ARGS;

public class BulkPortal implements Portal {

    private String name;
    private String query;
    private Statement statement;
    private SQLOperations.Session session;
    private Analysis analysis;
    private int maxRows = 0;
    private RowReceiverToResultReceiver rowReceiver = null;
    private List<? extends DataType> outputTypes;
    private final List<List<Object>> bulkParams = new ArrayList<>();
    private final List<ResultReceiver> resultReceivers = new ArrayList<>();

    public BulkPortal(String name, SQLOperations.Session session) {
        this.name = name;
        this.session = session;
    }

    public BulkPortal(SimplePortal simplePortal) {
        this.name = simplePortal.name;
        this.query = simplePortal.query;
        this.statement = simplePortal.statement;
        this.session = simplePortal.session;
        this.analysis = simplePortal.analysis;
        this.maxRows = simplePortal.maxRows;
        this.rowReceiver = simplePortal.rowReceiver;
        this.outputTypes = simplePortal.getOutputTypes();
        this.bulkParams.add(simplePortal.params);
        this.resultReceivers.add(simplePortal.resultReceiver);
    }

    @Override
    public PortalType type() {
        return PortalType.BULK;
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
        if (bulkParams.size() == 1) {
            return bulkParams.get(0).toArray(new Object[0]);
        } else {
            return EMPTY_ARGS;
        }
    }

    @Override
    public FormatCodes.FormatCode[] getResultFormatCodes() {
        return new FormatCodes.FormatCode[0];
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
        this.bulkParams.add(params);
    }

    @Override
    public void addResultFormatCodes(@Nullable FormatCodes.FormatCode[] resultFormatCodes) {}

    @Override
    public void addOutputTypes(List<? extends DataType> outputTypes) {
        this.outputTypes = outputTypes;
    }

    @Override
    public void addResultReceiver(ResultReceiver resultReceiver) {
        this.resultReceivers.add(resultReceiver);
    }

    @Override
    public void setRowReceiver(RowReceiverToResultReceiver rowReceiver) {
        this.rowReceiver = rowReceiver;
    }

    @Override
    public void setMaxRows(int maxRows) {
        this.maxRows = maxRows;
    }

    @Override
    public void execute(Analyzer analyzer, Planner planner, StatsTables statsTables, CompletionListener listener) {
        Object[][] bulkArgs = toBulkArgs(bulkParams);
        Analysis analysis = analyzer.analyze(statement, new ParameterContext(new Object[0],
                                                                             bulkArgs,
                                                                             session.getDefaultSchema()));
        Plan plan = planner.plan(analysis, session.jobId(), 0, maxRows);
        executeBulk(session.getExecutor(), plan, session.jobId(), statsTables, listener);
    }

    private void executeBulk(Executor executor, Plan plan, final UUID jobId,
                            final StatsTables statsTables, final CompletionListener listener) {
        List<? extends ListenableFuture<TaskResult>> futures = executor.executeBulk(plan);
        Futures.addCallback(Futures.allAsList(futures), new FutureCallback<List<TaskResult>>() {
            @Override
            public void onSuccess(@Nullable List<TaskResult> result) {
                assert result != null && result.size() == resultReceivers.size()
                    : "number of result must match number of rowReceivers";

                for (int i = 0; i < result.size(); i++) {
                    ResultReceiver resultReceiver = resultReceivers.get(i);
                    resultReceiver.setNextRow(result.get(i).rows().iterator().next());
                    resultReceiver.allFinished();
                }
                listener.onSuccess(null);
                statsTables.jobFinished(jobId, null);
            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
                for (ResultReceiver resultReceiver : resultReceivers) {
                    resultReceiver.fail(t);
                }
                listener.onFailure(t);
                statsTables.jobFinished(jobId, Exceptions.messageOf(t));
            }
        });
    }

    public void close() {
        if (rowReceiver != null) {
            ResumeHandle resumeHandle = rowReceiver.resumeHandle();
            if (resumeHandle != null) {
                rowReceiver.kill(new InterruptedException("Client closed portal"));
                resumeHandle.resume(false);
            }
        }
    }

    private static Object[][] toBulkArgs(List<List<Object>> bulkParams) {
        Object[][] bulkArgs = new Object[bulkParams.size()][];
        for (int i = 0; i < bulkArgs.length; i++) {
            bulkArgs[i] = bulkParams.get(i).toArray(new Object[0]);
        }
        return bulkArgs;
    }
}
