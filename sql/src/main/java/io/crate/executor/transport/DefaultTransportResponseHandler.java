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

package io.crate.executor.transport;

import com.google.common.base.MoreObjects;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;

public abstract class DefaultTransportResponseHandler<TResponse extends TransportResponse>
    implements TransportResponseHandler<TResponse> {

    private final ActionListener<TResponse> listener;
    private final String executor;

    /**
     * Creates a ResponseHandler that passes the response or exception to the given listener.
     * onResponse/onFailure will be executed using the SAME threadPool
     */
    protected DefaultTransportResponseHandler(ActionListener<TResponse> listener) {
        this(listener, null);
    }

    /**
     * Creates a ResponseHandler that passes the response or exception to the given listener.
     * onResponse/onFailure will be executed using the given executor or SAME if the executor is null
     */
    protected DefaultTransportResponseHandler(ActionListener<TResponse> listener, @Nullable String executor) {
        this.listener = listener;
        this.executor = MoreObjects.firstNonNull(executor, ThreadPool.Names.SAME);
    }

    @Override
    public void handleResponse(TResponse response) {
        listener.onResponse(response);
    }

    @Override
    public void handleException(TransportException exp) {
        listener.onFailure(exp);
    }

    @Override
    public String executor() {
        return executor;
    }
}
