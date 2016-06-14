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

package io.crate.action.sql.fetch;

import io.crate.action.ActionListeners;
import io.crate.action.sql.SQLActionException;
import io.crate.core.collections.Bucket;
import io.crate.operation.ClientPagingReceiver;
import io.crate.operation.HandlerOperations;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportService;

import java.util.Locale;

@Singleton
public class TransportClientFetchAction extends TransportAction<FetchRequest, FetchResponse> {

    private final static String ACTION = "sql_fetch";
    private final static String EXECUTOR = ThreadPool.Names.SEARCH;
    private final HandlerOperations handlerOperations;

    @Inject
    public TransportClientFetchAction(Settings settings,
                                      String actionName,
                                      ThreadPool threadPool,
                                      TransportService transportService,
                                      ActionFilters actionFilters,
                                      IndexNameExpressionResolver indexNameExpressionResolver,
                                      HandlerOperations handlerOperations) {
        super(settings, actionName, threadPool, actionFilters, indexNameExpressionResolver, transportService.getTaskManager());
        this.handlerOperations = handlerOperations;
        transportService.registerRequestHandler(ACTION, FetchRequest.class, EXECUTOR, new TransportHandler());
    }

    @Override
    protected void doExecute(FetchRequest request, final ActionListener<FetchResponse> listener) {
        final ClientPagingReceiver receiver = handlerOperations.get(request.cursorId(), request.fetchProperties().cursorKeepAlive());
        if (receiver == null) {
            listener.onFailure(new SQLActionException(String.format(Locale.ENGLISH,
                "No context for cursorId \"%s\" found.", request.cursorId()), 4000, RestStatus.BAD_REQUEST));
            return;
        }
        receiver.fetch(request.fetchProperties(), new ClientPagingReceiver.FetchCallback() {
            @Override
            public void onResult(Bucket rows, boolean isLast) {
                listener.onResponse(new FetchResponse(rows,  isLast, receiver.outputTypes()));
            }

            @Override
            public void onError(Throwable t) {
                listener.onFailure(t);
            }
        });
    }

    private class TransportHandler extends org.elasticsearch.transport.TransportRequestHandler<FetchRequest> {
        @Override
        public void messageReceived(FetchRequest request, TransportChannel channel) throws Exception {
            ActionListener<FetchResponse> listener = ActionListeners.forwardTo(channel);
            doExecute(request, listener);
        }
    }
}
