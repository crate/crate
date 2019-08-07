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

package org.elasticsearch.indices.recovery;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.FutureTransportResponseHandler;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;

import java.io.IOException;

public class RecoveryTranslogOperationsResponse extends TransportResponse {

    final long localCheckpoint;

    RecoveryTranslogOperationsResponse(final long localCheckpoint) {
        this.localCheckpoint = localCheckpoint;
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeZLong(localCheckpoint);
    }

    public RecoveryTranslogOperationsResponse(final StreamInput in) throws IOException {
        localCheckpoint = in.readZLong();
    }

    static final TransportResponseHandler<RecoveryTranslogOperationsResponse> HANDLER =
        new FutureTransportResponseHandler<>() {
            @Override
            public RecoveryTranslogOperationsResponse read(StreamInput in) throws IOException {
                return new RecoveryTranslogOperationsResponse(in);
            }
        };

}
