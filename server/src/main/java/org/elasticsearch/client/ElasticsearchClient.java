/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client;


import java.util.concurrent.CompletableFuture;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportResponse;

public interface ElasticsearchClient {

    /**
     * Executes a generic action, denoted by an {@link ActionType}.
     *
     * @param action           The action type to execute.
     * @param request          The action request.
     * @param <Request>        The request type.
     * @param <Response>       The response type.
     */
    <Req extends TransportRequest, Resp extends TransportResponse> CompletableFuture<Resp> execute(ActionType<Resp> action, Req request);

    /**
     * Returns the threadpool used to execute requests on this client
     */
    ThreadPool threadPool();

}
