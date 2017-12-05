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
import io.crate.analyze.ParameterContext;
import io.crate.analyze.symbol.Field;
import io.crate.analyze.symbol.Symbols;
import io.crate.concurrent.CountdownFutureCallback;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.data.RowN;
import io.crate.exceptions.ReadOnlyException;
import io.crate.exceptions.SQLExceptions;
import io.crate.metadata.RoutingProvider;
import io.crate.metadata.TransactionContext;
import io.crate.operation.collect.stats.JobsLogs;
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

class BatchPortal extends AbstractPortal {

    private final List<List<Object>> batchParams = new ArrayList<>();
    private final List<String> queries = new ArrayList<>();
    private final List<Analysis> analysis = new ArrayList<>();
    private final List<FormatCodes.FormatCode[]> resultFormatCodes = new ArrayList<>();
    private final List<List<? extends DataType>> outputTypes = new ArrayList<>();
    private final List<ResultReceiver> resultReceivers = new ArrayList<>();

    private TransactionContext transactionContext;

    BatchPortal(String name,
                String query,
                Analysis analysis,
                List<? extends DataType> outputTypes,
                ResultReceiver resultReceiver,
                List<Object> params,
                SessionContext sessionContext,
                PortalContext portalContext) {
        super(name, sessionContext, portalContext);
        queries.add(query);
        this.analysis.add(analysis);
        this.outputTypes.add(outputTypes);
        resultReceivers.add(resultReceiver);
        batchParams.add(params);
    }

    @Override
    @Nullable
    public FormatCodes.FormatCode[] getLastResultFormatCodes() {
        if (resultFormatCodes.isEmpty()) {
            return null;
        } else {
            return resultFormatCodes.get(resultFormatCodes.size() - 1);
        }
    }

    @Override
    public List<? extends DataType> getLastOutputTypes() {
        return outputTypes.get(outputTypes.size() - 1);
    }

    @Override
    public String getLastQuery() {
        return queries.get(queries.size() - 1);
    }

    @Override
    public Portal bind(String statementName, String query, Statement statement,
                       List<Object> params, @Nullable FormatCodes.FormatCode[] resultFormatCodes) {
        transactionContext = new TransactionContext(sessionContext);
        queries.add(query);
        batchParams.add(params);
        this.resultFormatCodes.add(resultFormatCodes);
        analysis.add(portalContext.getAnalyzer().boundAnalyze(
            statement, transactionContext, new ParameterContext(getArgs(), Collections.emptyList())));
        return this;
    }

    @Override
    public List<Field> describe() {
        Analysis lastAnalysis = analysis.get(analysis.size() - 1);
        if (lastAnalysis.rootRelation() == null) {
            return null;
        }
        List<Field> fields = lastAnalysis.rootRelation().fields();
        outputTypes.add(Symbols.typeView(fields));
        return fields;
    }

    @Override
    public void execute(ResultReceiver resultReceiver, int maxRows) {
        validate(analysis.get(analysis.size() - 1));
        resultReceivers.add(resultReceiver);
    }

    @Override
    public CompletableFuture<Void> sync(Planner planner, JobsLogs jobsLogs) {
        CountdownFutureCallback completionCallback = new CountdownFutureCallback(analysis.size());
        for (int i = 0; i < analysis.size(); i++) {
            UUID jobId = UUID.randomUUID();
            RoutingProvider routingProvider = new RoutingProvider(Randomness.get().nextInt(), planner.getAwarenessAttributes());
            PlannerContext plannerContext = new PlannerContext(
                planner.currentClusterState(),
                routingProvider,
                jobId,
                planner.functions(),
                transactionContext,
                0,
                0
            );
            Plan plan;
            String stmt = queries.get(i);
            try {
                plan = planner.plan(analysis.get(i).analyzedStatement(), plannerContext);
            } catch (Throwable t) {
                jobsLogs.logPreExecutionFailure(jobId, stmt, SQLExceptions.messageOf(t), sessionContext.user());
                throw t;
            }
            ResultReceiver resultReceiver = resultReceivers.get(i);
            jobsLogs.logExecutionStart(jobId, stmt, sessionContext.user());
            JobsLogsUpdateListener jobsLogsUpdateListener = new JobsLogsUpdateListener(jobId, jobsLogs);

            resultReceiver.completionFuture()
                .whenComplete(jobsLogsUpdateListener)
                .whenComplete(completionCallback);

            RowConsumer consumer = new RowConsumerToResultReceiver(resultReceiver, 0);
            plan.execute(
                portalContext.getExecutor(),
                plannerContext,
                consumer,
                new RowN(batchParams.toArray()),
                Collections.emptyMap()
            );
        }
        synced = true;
        return completionCallback;
    }

    private Row getArgs() {
        return new RowN(batchParams.get(batchParams.size() - 1).toArray());
    }

    private void validate(Analysis analysis) {
        AnalyzedStatement analyzedStatement = analysis.analyzedStatement();
        if (!analyzedStatement.isWriteOperation()) {
            throw new UnsupportedOperationException("Only write operations are allowed in Batch statements");
        }
        if (portalContext.isReadOnly()) {
            throw new ReadOnlyException();
        }
    }
}
