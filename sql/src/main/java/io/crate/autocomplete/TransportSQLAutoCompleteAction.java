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

package io.crate.autocomplete;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.action.sql.TransportSQLAction;
import io.crate.executor.transport.ResponseForwarder;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BaseTransportRequestHandler;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportService;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class TransportSQLAutoCompleteAction extends TransportAction<SQLAutoCompleteRequest, SQLAutoCompleteResponse> {

    private final AutoCompleter autoCompleter;

    @Inject
    public TransportSQLAutoCompleteAction(Settings settings,
                                          TransportSQLAction transportSQLAction,
                                          TransportService transportService,
                                          ThreadPool threadPool) {
        super(settings, SQLAutoCompleteAction.NAME, threadPool);
        this.autoCompleter = new AutoCompleter(new InformationSchemaDataProvider(transportSQLAction));
        transportService.registerHandler(SQLAutoCompleteAction.NAME, new TransportHandler());
    }

    @Override
    protected void doExecute(SQLAutoCompleteRequest request, final ActionListener<SQLAutoCompleteResponse> listener) {
        try {
            ListenableFuture<CompletionResult> completionFuture = autoCompleter.complete(request.statement());
            Futures.addCallback(completionFuture, new FutureCallback<CompletionResult>() {
                @Override
                public void onSuccess(@Nullable CompletionResult result) {
                    assert result != null;
                    listener.onResponse(new SQLAutoCompleteResponse(result.startIdx(), result.completions()));
                }

                @Override
                public void onFailure(@Nonnull Throwable t) {
                    listener.onFailure(t);
                }
            });
        } catch (Throwable e) {
            listener.onFailure(e);
        }
    }

    private class TransportHandler extends BaseTransportRequestHandler<SQLAutoCompleteRequest> {
        @Override
        public SQLAutoCompleteRequest newInstance() {
            return new SQLAutoCompleteRequest();
        }

        @Override
        public void messageReceived(SQLAutoCompleteRequest request, TransportChannel channel) throws Exception {
            ActionListener<SQLAutoCompleteResponse> listener = ResponseForwarder.forwardTo(channel);
            execute(request, listener);
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SAME;
        }
    }
}
