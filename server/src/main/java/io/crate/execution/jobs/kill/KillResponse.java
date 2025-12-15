/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.execution.jobs.kill;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.TransportResponse;

import org.jspecify.annotations.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.function.Function;

public class KillResponse extends TransportResponse {

    static final MergeKillResponseFunction MERGE_FUNCTION = new MergeKillResponseFunction();

    private final long numKilled;

    KillResponse(long numKilled) {
        this.numKilled = numKilled;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(numKilled);
    }

    public KillResponse(StreamInput in) throws IOException {
        numKilled = in.readVLong();
    }

    public long numKilled() {
        return numKilled;
    }


    static class MergeKillResponseFunction implements Function<List<KillResponse>, KillResponse> {
        @Override
        public KillResponse apply(@Nullable List<KillResponse> input) {
            if (input == null) {
                return new KillResponse(0);
            }
            long numKilled = 0;
            for (KillResponse killResponse : input) {
                numKilled += killResponse.numKilled();
            }
            return new KillResponse(numKilled);
        }
    }
}
