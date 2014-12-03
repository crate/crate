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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportResponse;

public class ResponseForwarder {

    private static final ESLogger logger = Loggers.getLogger(ResponseForwarder.class);

    public static <Response extends TransportResponse> ActionListener<Response> forwardTo(final TransportChannel channel) {
        return new ActionListener<Response>() {
            @Override
            public void onResponse(Response response) {
                try {
                    channel.sendResponse(response);
                } catch (Exception e) {
                    onFailure(e);
                }
            }

            @Override
            public void onFailure(Throwable e) {
                try {
                    channel.sendResponse(e);
                } catch (Exception e1) {
                    logger.error(String.format("error sending failure: %s", e.toString()), e1);
                }
            }
        };
    }
}
