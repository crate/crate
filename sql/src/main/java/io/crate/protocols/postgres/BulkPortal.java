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
import io.crate.analyze.Analysis;
import io.crate.analyze.ParameterContext;
import io.crate.analyze.symbol.Field;
import io.crate.concurrent.CompletionListener;
import io.crate.exceptions.Exceptions;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.executor.Executor;
import io.crate.executor.TaskResult;
import io.crate.operation.collect.StatsTables;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.sql.tree.Statement;
import io.crate.types.DataType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

class BulkPortal extends AbstractPortal {

    private final List<List<Object>> bulkParams = new ArrayList<>();
    private final List<ResultReceiver> resultReceivers = new ArrayList<>();
    private String query;
    private Statement statement;
    private int maxRows = 0;
    private List<? extends DataType> outputTypes;

    BulkPortal(String name,
               String query,
               Statement statement,
               List<? extends DataType> outputTypes,
               ResultReceiver resultReceiver,
               int maxRows,
               List<Object> params,
               SessionData sessionData) {
        super(name, sessionData);
        this.query = query;
        this.statement = statement;
        this.outputTypes = outputTypes;
        this.resultReceivers.add(resultReceiver);
        this.maxRows = maxRows;
        this.bulkParams.add(params);
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
        return this.query;
    }

    @Override
    public Portal bind(String statementName,
                       String query,
                       Statement statement,
                       List<Object> params,
                       @Nullable FormatCodes.FormatCode[] resultFormatCodes) {
        this.bulkParams.add(params);
        return this;
    }

    @Override
    public List<Field> describe() {
        return null;
    }

    @Override
    public void execute(ResultReceiver resultReceiver, int maxRows) {
        if (maxRows != 1) {
            throw new UnsupportedFeatureException("bulk operations don't support fetch size");
        }
        this.resultReceivers.add(resultReceiver);
        this.maxRows = maxRows;
    }

    @Override
    public void sync(Planner planner, StatsTables statsTables, CompletionListener listener) {
        Object[][] bulkArgs = toBulkArgs(bulkParams);
        Analysis analysis = sessionData.getAnalyzer().analyze(statement,
            new ParameterContext(new Object[0],
                bulkArgs,
                sessionData.getDefaultSchema()));
        Plan plan = planner.plan(analysis, sessionData.getJobId(), 0, maxRows);
        executeBulk(sessionData.getExecutor(), plan, sessionData.getJobId(), statsTables, listener);
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

    private static Object[][] toBulkArgs(List<List<Object>> bulkParams) {
        Object[][] bulkArgs = new Object[bulkParams.size()][];
        for (int i = 0; i < bulkArgs.length; i++) {
            bulkArgs[i] = bulkParams.get(i).toArray(new Object[0]);
        }
        return bulkArgs;
    }
}
