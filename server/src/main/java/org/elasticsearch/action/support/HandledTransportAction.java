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

package org.elasticsearch.action.support;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

/**
 * A TransportAction that self registers a handler into the transport service
 */
public abstract class HandledTransportAction<Request extends TransportRequest, Response extends TransportResponse>
        extends TransportAction<Request, Response> {

    protected final Logger logger = LogManager.getLogger(getClass());

    protected HandledTransportAction(String actionName,
                                     TransportService transportService,
                                     Writeable.Reader<Request> reader) {
        this(actionName, true, transportService, reader);
    }

    protected HandledTransportAction(String actionName,
                                     boolean canTripCircuitBreaker,
                                     TransportService transportService,
                                     Writeable.Reader<Request> requestReader) {
        super(actionName);
        transportService.registerRequestHandler(
            actionName,
            ThreadPool.Names.SAME,
            false,
            canTripCircuitBreaker,
            requestReader,
            new TransportHandler()
        );
    }

    class TransportHandler implements TransportRequestHandler<Request> {
        @Override
        public final void messageReceived(final Request request, final TransportChannel channel) throws Exception {
            execute(request).whenComplete(new ChannelActionListener<>(channel, actionName, request));
        }
    }
}
