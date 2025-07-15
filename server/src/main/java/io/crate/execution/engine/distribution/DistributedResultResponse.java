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

package io.crate.execution.engine.distribution;

import java.io.IOException;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.TransportResponse;

public class DistributedResultResponse extends TransportResponse {

    public static enum Result {
        NEED_MORE,
        TASK_MISSING,
        DONE
    }

    private final Result result;

    public DistributedResultResponse(Result result) {
        this.result = result;
    }

    public Result result() {
        return result;
    }

    public DistributedResultResponse(StreamInput in) throws IOException {
        if (in.getVersion().onOrAfter(Version.V_6_0_0)) {
            this.result = in.readEnum(Result.class);
        } else {
            boolean needMore = in.readBoolean();
            result = needMore ? Result.NEED_MORE : Result.DONE;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getVersion().onOrAfter(Version.V_6_0_0)) {
            out.writeEnum(result);
        } else {
            switch (result) {
                case DONE:
                    out.writeBoolean(false);
                    break;
                case NEED_MORE:
                    out.writeBoolean(true);
                    break;
                case TASK_MISSING:
                    out.writeBoolean(false);
                    break;
                default:
                    break;
            }
        }
    }
}
