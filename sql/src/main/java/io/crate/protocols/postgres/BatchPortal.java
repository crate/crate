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
import io.crate.action.sql.RowReceiverToResultReceiver;
import io.crate.analyze.Analysis;
import io.crate.analyze.AnalyzedStatement;
import io.crate.analyze.ParameterContext;
import io.crate.analyze.symbol.Field;
import io.crate.analyze.symbol.Symbols;
import io.crate.concurrent.CompletionListener;
import io.crate.concurrent.CompletionMultiListener;
import io.crate.concurrent.CompletionState;
import io.crate.exceptions.BatchModeWriteOnlyException;
import io.crate.exceptions.ReadOnlyException;
import io.crate.operation.collect.StatsTables;
import io.crate.operation.projectors.RowReceiver;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.sql.tree.Statement;
import io.crate.types.DataType;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static io.crate.action.sql.SQLBulkRequest.EMPTY_BULK_ARGS;

class BatchPortal extends AbstractPortal {

    private final static ESLogger LOGGER = Loggers.getLogger(BatchPortal.class);

    private List<List<Object>> batchParams = new ArrayList<>();
    private List<String> queries = new ArrayList<>();
    private List<Analysis> analysis= new ArrayList<>();
    private List<FormatCodes.FormatCode[]> resultFormatCodes = new ArrayList<>();
    private List<List<? extends DataType>> outputTypes = new ArrayList<>();
    private List<ResultReceiver> resultReceivers = new ArrayList<>();

    BatchPortal(String name,
                String query,
                Analysis analysis,
                List<? extends DataType> outputTypes,
                ResultReceiver resultReceiver,
                List<Object> params,
                SessionData sessionData) {
        super(name, sessionData);
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
        queries.add(query);
        batchParams.add(params);
        this.resultFormatCodes.add(resultFormatCodes);
        analysis.add(sessionData.getAnalyzer().analyze(statement,
            new ParameterContext(getArgs(),
                EMPTY_BULK_ARGS,
                sessionData.getDefaultSchema())));
        return this;
    }

    @Override
    public List<Field> describe() {
        Analysis lastAnalysis = analysis.get(analysis.size() - 1);
        if (lastAnalysis.rootRelation() == null) {
            return null;
        }
        List<Field> fields = lastAnalysis.rootRelation().fields();
        outputTypes.add(Symbols.extractTypes(fields));
        return fields;
    }

    @Override
    public void execute(ResultReceiver resultReceiver, int maxRows) {
        validate(analysis.get(analysis.size() - 1));
        resultReceivers.add(resultReceiver);
    }

    @Override
    public void sync(Planner planner, StatsTables statsTables, CompletionListener listener) {
        BatchCompletionListener batchCompletionListener = new BatchCompletionListener(analysis.size(), listener,
            new StatsTablesUpdateListener(sessionData.getJobId(), statsTables));
        for (int i = 0; i < analysis.size(); i++) {
            Plan plan = planner.plan(analysis.get(i), UUID.randomUUID(), 0, 0);
            ResultReceiver resultReceiver = resultReceivers.get(i);
            resultReceiver.addListener(batchCompletionListener);
            RowReceiver rowReceiver = new RowReceiverToResultReceiver(resultReceiver, 0);
            sessionData.getExecutor().execute(plan, rowReceiver);
        }
    }

    private Object[] getArgs() {
        return batchParams.get(batchParams.size() - 1).toArray(new Object[0]);
    }

    private void validate(Analysis analysis) {
        AnalyzedStatement analyzedStatement = analysis.analyzedStatement();
        if (!analyzedStatement.isWriteOperation()) {
            throw new BatchModeWriteOnlyException();
        }
        if (sessionData.isReadOnly()) {
            throw new ReadOnlyException();
        }
    }

    private static class BatchCompletionListener implements CompletionListener {

        private final AtomicInteger counter;
        private final CompletionListener delegate;

        private BatchCompletionListener(int noOfActions,
                                        CompletionListener completionListener,
                                        CompletionListener statsTablesCompletionListener) {
            this.counter = new AtomicInteger(noOfActions);
            this.delegate = CompletionMultiListener.merge(completionListener, statsTablesCompletionListener);
        }

        @Override
        public void onSuccess(@Nullable CompletionState result) {
            if (counter.decrementAndGet() == 0) {
                this.delegate.onSuccess(result);
            }
        }

        @Override
        public void onFailure(Throwable t) {
            if (counter.decrementAndGet() == 0) {
                this.delegate.onFailure(t);
            }
        }
    }
}
