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

package org.elasticsearch.action.support.nodes;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import io.crate.common.unit.TimeValue;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;

public abstract class BaseNodesRequest<Request extends BaseNodesRequest<Request>> extends TransportRequest {

    private final DiscoveryNode[] concreteNodes;

    private TimeValue timeout;

    protected BaseNodesRequest(DiscoveryNode... concreteNodes) {
        this.concreteNodes = concreteNodes;
    }

    public TimeValue timeout() {
        return this.timeout;
    }

    @SuppressWarnings("unchecked")
    public final Request timeout(TimeValue timeout) {
        this.timeout = timeout;
        return (Request) this;
    }

    public DiscoveryNode[] concreteNodes() {
        return concreteNodes;
    }

    public BaseNodesRequest(StreamInput in) throws IOException {
        super(in);
        if (in.getVersion().before(Version.V_4_1_0)) {
            in.readStringArray(); // node-ids
        }
        concreteNodes = in.readOptionalArray(DiscoveryNode::new, DiscoveryNode[]::new);
        timeout = in.readOptionalTimeValue();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getVersion().before(Version.V_4_1_0)) {
            out.writeStringArrayNullable(null);
        }
        out.writeOptionalArray(concreteNodes);
        out.writeOptionalTimeValue(timeout);
    }
}
