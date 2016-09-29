/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.action.sql;

import io.crate.action.ActionListeners;
import io.crate.analyze.symbol.Field;
import io.crate.concurrent.CompletionListener;
import io.crate.concurrent.CompletionMultiListener;
import io.crate.core.collections.Row;
import io.crate.exceptions.Exceptions;
import io.crate.executor.BytesRefUtils;
import io.crate.types.DataType;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportService;

import javax.annotation.Nonnull;
import java.util.*;


@Singleton
public class TransportSQLAction extends TransportAction<SQLRequest, SQLResponse> {

    private static final DataType[] EMPTY_TYPES = new DataType[0];
    private static final String[] EMPTY_NAMES = new String[0];
    private static final Object[][] EMPTY_ROWS = new Object[0][];

    private final static String UNNAMED = "";
    final static int DEFAULT_SOFT_LIMIT = 10_000;

    private final SQLOperations sqlOperations;

    @Inject
    public TransportSQLAction(SQLOperations sqlOperations,
                              Settings settings,
                              ThreadPool threadPool,
                              TransportService transportService,
                              ActionFilters actionFilters,
                              IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, SQLAction.NAME, threadPool, actionFilters, indexNameExpressionResolver, transportService.getTaskManager());
        this.sqlOperations = sqlOperations;
        transportService.registerRequestHandler(SQLAction.NAME, SQLRequest.class, ThreadPool.Names.SAME, new TransportHandler());
    }

    static Set<Option> toOptions(int requestFlags) {
        switch (requestFlags) {
            case SQLBaseRequest.HEADER_FLAG_OFF:
                return Option.NONE;
            case SQLBaseRequest.HEADER_FLAG_ALLOW_QUOTED_SUBSCRIPT:
                return EnumSet.of(Option.ALLOW_QUOTED_SUBSCRIPT);
        }
        throw new IllegalArgumentException("Unrecognized requestFlags: " + requestFlags);
    }

    @Override
    protected void doExecute(SQLRequest request, ActionListener<SQLResponse> listener) {
        SQLOperations.Session session = sqlOperations.createSession(
            request.getDefaultSchema(), toOptions(request.getRequestFlags()), DEFAULT_SOFT_LIMIT);
        try {
            long startTime = System.nanoTime();
            session.parse(UNNAMED, request.stmt(), Collections.<DataType>emptyList());
            List<Object> args = request.args() == null ? Collections.emptyList() : Arrays.asList(request.args());
            session.bind(UNNAMED, UNNAMED, args, null);
            List<Field> outputFields = session.describe('P', UNNAMED);
            if (outputFields == null) {
                ResultReceiver resultReceiver = new RowCountReceiver(listener, startTime, request.includeTypesOnResponse());
                session.execute(UNNAMED, 1, resultReceiver);
            } else {
                ResultReceiver resultReceiver = new ResultSetReceiver(listener, outputFields, startTime, request.includeTypesOnResponse());
                session.execute(UNNAMED, 0, resultReceiver);
            }
            session.sync(CompletionListener.NO_OP);
        } catch (Throwable t) {
            listener.onFailure(Exceptions.createSQLActionException(t));
        }
    }

    private static class ResultSetReceiver implements ResultReceiver {

        private final List<Object[]> rows = new ArrayList<>();
        private final ActionListener<SQLResponse> listener;
        private final List<Field> outputFields;
        private final long startTime;
        private final boolean includeTypesOnResponse;
        private CompletionListener completionListener = CompletionListener.NO_OP;

        ResultSetReceiver(ActionListener<SQLResponse> listener,
                          List<Field> outputFields,
                          long startTime,
                          boolean includeTypesOnResponse) {
            this.listener = listener;
            this.outputFields = outputFields;
            this.startTime = startTime;
            this.includeTypesOnResponse = includeTypesOnResponse;
        }

        @Override
        public void addListener(CompletionListener listener) {
            this.completionListener = CompletionMultiListener.merge(this.completionListener, listener);
        }

        @Override
        public void setNextRow(Row row) {
            rows.add(row.materialize());
        }

        @Override
        public void batchFinished() {
        }

        @Override
        public void allFinished() {
            listener.onResponse(createSqlResponse());
            completionListener.onSuccess(null);
        }

        @Override
        public void fail(@Nonnull Throwable t) {
            listener.onFailure(Exceptions.createSQLActionException(t));
            completionListener.onFailure(t);
        }

        private SQLResponse createSqlResponse() {
            String[] outputNames = new String[outputFields.size()];
            DataType[] outputTypes = new DataType[outputFields.size()];

            for (int i = 0, outputFieldsSize = outputFields.size(); i < outputFieldsSize; i++) {
                Field field = outputFields.get(i);
                outputNames[i] = field.path().outputName();
                outputTypes[i] = field.valueType();
            }

            Object[][] rowsArr = rows.toArray(new Object[0][]);
            BytesRefUtils.ensureStringTypesAreStrings(outputTypes, rowsArr);
            float duration = (float) ((System.nanoTime() - startTime) / 1_000_000.0);
            return new SQLResponse(
                outputNames,
                rowsArr,
                outputTypes,
                rowsArr.length,
                duration,
                includeTypesOnResponse
            );
        }
    }

    private static class RowCountReceiver implements ResultReceiver {

        private final ActionListener<SQLResponse> listener;
        private final long startTime;
        private final boolean includeTypes;

        private long rowCount;
        private CompletionListener completionListener = CompletionListener.NO_OP;

        RowCountReceiver(ActionListener<SQLResponse> listener, long startTime, boolean includeTypes) {
            this.listener = listener;
            this.startTime = startTime;
            this.includeTypes = includeTypes;
        }

        @Override
        public void addListener(CompletionListener listener) {
            this.completionListener = CompletionMultiListener.merge(this.completionListener, listener);
        }

        @Override
        public void setNextRow(Row row) {
            rowCount = (long) row.get(0);
        }

        @Override
        public void batchFinished() {
        }

        @Override
        public void allFinished() {
            float duration = (float) ((System.nanoTime() - startTime) / 1_000_000.0);
            SQLResponse sqlResponse = new SQLResponse(
                EMPTY_NAMES,
                EMPTY_ROWS,
                EMPTY_TYPES,
                rowCount,
                duration,
                includeTypes
            );
            listener.onResponse(sqlResponse);
            completionListener.onSuccess(null);

        }

        @Override
        public void fail(@Nonnull Throwable t) {
            listener.onFailure(Exceptions.createSQLActionException(t));
            completionListener.onFailure(t);
        }
    }

    private class TransportHandler extends TransportRequestHandler<SQLRequest> {
        @Override
        public void messageReceived(SQLRequest request, final TransportChannel channel) throws Exception {
            ActionListener<SQLResponse> listener = ActionListeners.forwardTo(channel);
            execute(request, listener);
        }
    }
}
