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
import io.crate.analyze.ParameterContext;
import io.crate.analyze.symbol.Field;
import io.crate.data.Row;
import io.crate.data.RowN;
import io.crate.data.Rows;
import io.crate.exceptions.SQLExceptions;
import io.crate.executor.Executor;
import io.crate.operation.collect.stats.JobsLogs;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.sql.tree.Statement;
import io.crate.types.DataType;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

class BulkPortal extends AbstractPortal {

    private final List<List<Object>> bulkArgs = new ArrayList<>();
    private final List<ResultReceiver> resultReceivers = new ArrayList<>();
    private String query;
    private Statement statement;
    private int maxRows = 0;
    private List<? extends DataType> outputTypes;
    private List<Field> fields;

    BulkPortal(String name,
               String query,
               Statement statement,
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
        return this.query;
    }

    @Override
    public Portal bind(String statementName,
                       String query,
                       Statement statement,
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
        Analysis analysis = portalContext.getAnalyzer().boundAnalyze(statement,
            sessionContext,
            new ParameterContext(Row.EMPTY, bulkParams));
        UUID jobId = UUID.randomUUID();
        Plan plan;
        try {
            plan = planner.plan(analysis, jobId, 0, maxRows);
        } catch (Throwable t) {
            jobsLogs.logPreExecutionFailure(jobId, query, SQLExceptions.messageOf(t), sessionContext.user());
            throw t;
        }
        jobsLogs.logExecutionStart(jobId, query, sessionContext.user());
        synced = true;
        return executeBulk(portalContext.getExecutor(), plan, jobId, jobsLogs, bulkParams);
    }

    private CompletableFuture<Void> executeBulk(Executor executor,
                                                Plan plan,
                                                final UUID jobId,
                                                final JobsLogs jobsLogs,
                                                List<Row> bulkParams) {
        List<CompletableFuture<Long>> futures = executor.executeBulk(plan, bulkParams);
        CompletableFuture<Void> allFutures = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
        return allFutures
            .exceptionally(t -> null) // swallow exception - failures are set per item in emitResults
            .thenAccept(ignored -> emitResults(jobId, jobsLogs, futures));
    }

    private void emitResults(UUID jobId, JobsLogs jobsLogs, List<CompletableFuture<Long>> completedResultFutures) {
        assert completedResultFutures.size() == resultReceivers.size()
            : "number of result must match number of rowReceivers";

        Long[] cells = new Long[1];
        RowN row = new RowN(cells);
        for (int i = 0; i < completedResultFutures.size(); i++) {
            CompletableFuture<Long> completedResultFuture = completedResultFutures.get(i);
            ResultReceiver resultReceiver = resultReceivers.get(i);
            try {
                Long rowCount = completedResultFuture.join();
                cells[0] = rowCount == null ? Executor.ROWCOUNT_ERROR : rowCount;
            } catch (Throwable t) {
                cells[0] = Executor.ROWCOUNT_ERROR;
            }
            resultReceiver.setNextRow(row);
            resultReceiver.allFinished(false);
        }
        jobsLogs.logExecutionEnd(jobId, null);
    }
}
