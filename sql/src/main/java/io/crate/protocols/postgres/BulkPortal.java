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
import com.google.common.util.concurrent.SettableFuture;
import io.crate.action.sql.ResultReceiver;
import io.crate.action.sql.SessionContext;
import io.crate.analyze.Analysis;
import io.crate.analyze.ParameterContext;
import io.crate.analyze.symbol.Field;
import io.crate.core.collections.Row;
import io.crate.core.collections.RowN;
import io.crate.core.collections.Rows;
import io.crate.exceptions.Exceptions;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.executor.Executor;
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
        if (maxRows != 1) {
            throw new UnsupportedFeatureException("bulk operations don't support fetch size");
        }
        this.resultReceivers.add(resultReceiver);
        this.maxRows = maxRows;
    }

    @Override
    public ListenableFuture<?> sync(Planner planner, StatsTables statsTables) {
        List<Row> bulkParams = Rows.of(bulkArgs);
        Analysis analysis = portalContext.getAnalyzer().boundAnalyze(statement,
            sessionContext,
            new ParameterContext(Row.EMPTY, bulkParams));
        UUID jobId = UUID.randomUUID();
        Plan plan;
        try {
            plan = planner.plan(analysis, jobId, 0, maxRows);
        } catch (Throwable t) {
            statsTables.logPreExecutionFailure(jobId, query, Exceptions.messageOf(t));
            throw t;
        }
        statsTables.logExecutionStart(jobId, query);
        return executeBulk(portalContext.getExecutor(), plan, jobId, statsTables);
    }

    private ListenableFuture<Void> executeBulk(Executor executor, Plan plan, final UUID jobId,
                             final StatsTables statsTables) {
        final SettableFuture<Void> future = SettableFuture.create();
        Futures.addCallback(executor.executeBulk(plan), new FutureCallback<List<Long>>() {
                @Override
                public void onSuccess(@Nullable List<Long> result) {
                    assert result != null && result.size() == resultReceivers.size()
                        : "number of result must match number of rowReceivers";

                    Long[] cells = new Long[1];
                    RowN row = new RowN(cells);
                    for (int i = 0; i < result.size(); i++) {
                        cells[0] = result.get(i);
                        ResultReceiver resultReceiver = resultReceivers.get(i);
                        resultReceiver.setNextRow(row);
                        resultReceiver.allFinished();
                    }
                    future.set(null);
                    statsTables.logExecutionEnd(jobId, null);
                }

                @Override
                public void onFailure(@Nonnull Throwable t) {
                    for (ResultReceiver resultReceiver : resultReceivers) {
                        resultReceiver.fail(t);
                    }
                    future.setException(t);
                    statsTables.logExecutionEnd(jobId, Exceptions.messageOf(t));
                }
            });
        return future;
    }
}
