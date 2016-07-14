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
import io.crate.analyze.Analyzer;
import io.crate.analyze.ParameterContext;
import io.crate.analyze.symbol.Field;
import io.crate.analyze.symbol.Symbols;
import io.crate.concurrent.CompletionListener;
import io.crate.exceptions.Exceptions;
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

import static io.crate.action.sql.SQLBulkRequest.EMPTY_BULK_ARGS;
import static io.crate.action.sql.SQLRequest.EMPTY_ARGS;

class BulkPortal extends AbstractPortal {

    private String query;
    private Statement statement;
    private Analysis analysis;
    private Plan plan;
    private int maxRows = 0;
    private List<? extends DataType> outputTypes;
    private final List<List<Object>> bulkParams = new ArrayList<>();
    private final List<ResultReceiver> resultReceivers = new ArrayList<>();

    BulkPortal(String name,
               String query,
               Statement statement,
               Analysis analysis,
               Plan plan,
               List<? extends DataType> outputTypes,
               ResultReceiver resultReceiver,
               int maxRows,
               List<Object> params,
               SessionData sessionData) {
        super(name, sessionData);
        this.query = query;
        this.statement = statement;
        this.analysis = analysis;
        this.plan = plan;
        this.outputTypes = outputTypes;
        this.resultReceivers.add(resultReceiver);
        this.maxRows = maxRows;
        this.bulkParams.add(params);
    }

    @Override
    public Type type() {
        return Type.BULK;
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
    public String getLastQuery() {
        return this.query;
    }

    @Override
    public Portal bind(String statementName, String query, Statement statement,
                List<Object> params, @Nullable FormatCodes.FormatCode[] resultFormatCodes) {
        this.query = query;
        this.statement = statement;
        this.bulkParams.add(params);
        if (analysis == null) {
            analysis = sessionData.getAnalyzer().analyze(statement,
                                                         new ParameterContext(getArgs(),
                                                                              EMPTY_BULK_ARGS,
                                                                              sessionData.getDefaultSchema()));
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
        this.resultReceivers.add(resultReceiver);
        this.maxRows = maxRows;
    }

    @Override
    public Plan prepareSync(Planner planner) {
        Object[][] bulkArgs = toBulkArgs(bulkParams);
        Analysis analysis = sessionData.getAnalyzer().analyze(statement,
                                                              new ParameterContext(new Object[0],
                                                                                   bulkArgs,
                                                                                   sessionData.getDefaultSchema()));
        plan = planner.plan(analysis, sessionData.getJobId(), 0, maxRows);
        return plan;
    }

    @Override
    public void sync(Analyzer analyzer, Planner planner, StatsTables statsTables, CompletionListener listener) {
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

    private Object[] getArgs() {
        if (bulkParams.size() == 1) {
            return bulkParams.get(0).toArray(new Object[0]);
        } else {
            return EMPTY_ARGS;
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
