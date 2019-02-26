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

package io.crate.es.indices.recovery;

import io.crate.es.common.io.stream.StreamInput;
import io.crate.es.common.io.stream.StreamOutput;
import io.crate.es.transport.FutureTransportResponseHandler;
import io.crate.es.transport.TransportResponse;
import io.crate.es.transport.TransportResponseHandler;

import java.io.IOException;

public class RecoveryTranslogOperationsResponse extends TransportResponse {

    long localCheckpoint;

    RecoveryTranslogOperationsResponse() {

    }

    RecoveryTranslogOperationsResponse(final long localCheckpoint) {
        this.localCheckpoint = localCheckpoint;
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeZLong(localCheckpoint);
    }

    @Override
    public void readFrom(final StreamInput in) throws IOException {
        localCheckpoint = in.readZLong();
    }

    static TransportResponseHandler<RecoveryTranslogOperationsResponse> HANDLER =
            new FutureTransportResponseHandler<RecoveryTranslogOperationsResponse>() {
                @Override
                public RecoveryTranslogOperationsResponse newInstance() {
                    return new RecoveryTranslogOperationsResponse();
                }
            };

}
