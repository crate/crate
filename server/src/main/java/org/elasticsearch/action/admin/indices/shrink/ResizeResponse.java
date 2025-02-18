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

package org.elasticsearch.action.admin.indices.shrink;

import java.io.IOException;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.master.ShardsAcknowledgedResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

/**
 * A response for a resize index action, either shrink or split index.
 */
public final class ResizeResponse extends ShardsAcknowledgedResponse {

    public ResizeResponse(boolean acknowledged, boolean shardsAcknowledged) {
        super(acknowledged, shardsAcknowledged);
    }

    ResizeResponse(StreamInput in) throws IOException {
        super(in);
        if (in.getVersion().before(Version.V_6_0_0)) {
            in.readString(); // index
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getVersion().before(Version.V_6_0_0)) {
            // dummy index, the index of the resizeResponse wasn't used in 5.10
            // mixed cluster with < 5.10 is not supported
            out.writeString("");
        }
    }
}
