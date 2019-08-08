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

package org.elasticsearch.action.admin.cluster.state;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.TransportResponse;

import java.io.IOException;

/**
 * The response for getting the cluster state.
 */
public class ClusterStateResponse extends TransportResponse {

    private final ClusterName clusterName;
    private final ClusterState clusterState;

    public ClusterStateResponse(ClusterName clusterName, ClusterState clusterState) {
        this.clusterName = clusterName;
        this.clusterState = clusterState;
    }

    /**
     * The requested cluster state.  Only the parts of the cluster state that were
     * requested are included in the returned {@link ClusterState} instance.
     */
    public ClusterState getState() {
        return this.clusterState;
    }

    /**
     * The name of the cluster.
     */
    public ClusterName getClusterName() {
        return this.clusterName;
    }

    public ClusterStateResponse(StreamInput in) throws IOException {
        clusterName = new ClusterName(in);
        clusterState = ClusterState.readFrom(in, null);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        clusterName.writeTo(out);
        clusterState.writeTo(out);
    }
}
