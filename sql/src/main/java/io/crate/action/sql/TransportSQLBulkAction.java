/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import io.crate.concurrent.CompletionState;
import io.crate.core.collections.Row;
import io.crate.exceptions.Exceptions;
import io.crate.types.DataType;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportService;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static io.crate.action.sql.TransportSQLAction.DEFAULT_SOFT_LIMIT;
import static io.crate.action.sql.TransportSQLAction.toOptions;

public class TransportSQLBulkAction extends TransportAction<SQLBulkRequest, SQLBulkResponse> {

    private final SQLOperations sqlOperations;
    private final static String UNNAMED = "";

    @Inject
    public TransportSQLBulkAction(SQLOperations sqlOperations,
                                  Settings settings,
                                  ThreadPool threadPool,
                                  TransportService transportService,
                                  ActionFilters actionFilters,
                                  IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, SQLBulkAction.NAME, threadPool, actionFilters, indexNameExpressionResolver, transportService.getTaskManager());
        this.sqlOperations = sqlOperations;
        transportService.registerRequestHandler(SQLBulkAction.NAME, SQLBulkRequest.class, ThreadPool.Names.SAME, new TransportHandler());
    }

    @Override
    protected void doExecute(SQLBulkRequest request, final ActionListener<SQLBulkResponse> listener) {
        SQLOperations.Session session = sqlOperations.createSession(
            request.getDefaultSchema(),
            toOptions(request.getRequestFlags()),
            DEFAULT_SOFT_LIMIT
        );
        try {
            final long startTime = System.nanoTime();
            session.parse(UNNAMED, request.stmt(), Collections.<DataType>emptyList());

            Object[][] bulkArgs = request.bulkArgs();
            final SQLBulkResponse.Result[] results = new SQLBulkResponse.Result[bulkArgs.length];
            if (results.length == 0) {
                session.bind(UNNAMED, UNNAMED, Collections.emptyList(), null);
                session.execute(UNNAMED, 1, ResultReceiver.NO_OP);
            } else {
                for (int i = 0; i < bulkArgs.length; i++) {
                    session.bind(UNNAMED, UNNAMED, Arrays.asList(bulkArgs[i]), null);
                    ResultReceiver resultReceiver = new RowCountReceiver(results, i);
                    session.execute(UNNAMED, 1, resultReceiver);
                }
            }
            List<Field> outputColumns = session.describe('P', UNNAMED);
            if (outputColumns != null) {
                throw new UnsupportedOperationException(
                    "Bulk operations for statements that return result sets is not supported");
            }
            session.sync(new CompletionListener() {
                @Override
                public void onSuccess(@Nullable CompletionState result) {
                    float duration = (float)((System.nanoTime() - startTime) / 1_000_000.0);
                    listener.onResponse(new SQLBulkResponse(results, duration));
                }

                @Override
                public void onFailure(@Nonnull Throwable t) {
                    listener.onFailure(Exceptions.createSQLActionException(t));
                }
            });
        } catch (Throwable t) {
            listener.onFailure(Exceptions.createSQLActionException(t));
        }
    }


    /**
     * @deprecated should become part of {@link SQLOperations} and
     * {@link io.crate.cluster.gracefulstop.DecommissioningService} also needs to enable it again if decommissioning is aborted
     */
    @Deprecated
    public void disable() {

    }

    private static class RowCountReceiver implements ResultReceiver {

        private final SQLBulkResponse.Result[] results;
        private final int resultIdx;
        private CompletionListener completionListener = CompletionListener.NO_OP;
        private long rowCount;

        RowCountReceiver(SQLBulkResponse.Result[] results, int resultIdx) {
            this.results = results;
            this.resultIdx = resultIdx;
        }

        @Override
        public void addListener(CompletionListener listener) {
            this.completionListener = CompletionMultiListener.merge(this.completionListener, listener);
        }

        @Override
        public void setNextRow(Row row) {
            rowCount = ((long) row.get(0));
        }

        @Override
        public void batchFinished() {
        }

        @Override
        public void allFinished() {
            results[resultIdx] = new SQLBulkResponse.Result(null, rowCount);
            completionListener.onSuccess(null);
        }

        @Override
        public void fail(@Nonnull Throwable t) {
            results[resultIdx] = new SQLBulkResponse.Result(Exceptions.messageOf(t), rowCount);
            completionListener.onFailure(t);
        }
    }

    private class TransportHandler extends TransportRequestHandler<SQLBulkRequest> {
        @Override
        public void messageReceived(SQLBulkRequest request, final TransportChannel channel) throws Exception {
            ActionListener<SQLBulkResponse> listener = ActionListeners.forwardTo(channel);
            execute(request, listener);
        }
    }
}
