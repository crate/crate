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

import java.io.IOException;
import java.util.Objects;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.TransportResponse;

/**
 * The response for getting the cluster state.
 */
public class ClusterStateResponse extends TransportResponse {

    private final ClusterName clusterName;
    private final ClusterState clusterState;
    private boolean waitForTimedOut = false;

    public ClusterStateResponse(ClusterName clusterName, ClusterState clusterState, boolean waitForTimedOut) {
        this.clusterName = clusterName;
        this.clusterState = clusterState;
        this.waitForTimedOut = waitForTimedOut;
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

    /**
     * Returns whether the request timed out waiting for a cluster state with a metadata version equal or
     * higher than the specified metadata.
     */
    public boolean isWaitForTimedOut() {
        return waitForTimedOut;
    }

    public ClusterStateResponse(StreamInput in) throws IOException {
        clusterName = new ClusterName(in);
        if (in.getVersion().onOrAfter(Version.V_4_4_0)) {
            clusterState = in.readOptionalWriteable(innerIn -> ClusterState.readFrom(innerIn, null));
            waitForTimedOut = in.readBoolean();
        } else {
            clusterState = ClusterState.readFrom(in, null);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        clusterName.writeTo(out);
        if (out.getVersion().onOrAfter(Version.V_4_4_0)) {
            out.writeOptionalWriteable(clusterState);
            out.writeBoolean(waitForTimedOut);
        } else {
            clusterState.writeTo(out);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClusterStateResponse response = (ClusterStateResponse) o;
        return waitForTimedOut == response.waitForTimedOut &&
            Objects.equals(clusterName, response.clusterName) &&
            // Best effort. Only compare cluster state version and master node id,
            // because cluster state doesn't implement equals()
            Objects.equals(getVersion(clusterState), getVersion(response.clusterState)) &&
            Objects.equals(getMasterNodeId(clusterState), getMasterNodeId(response.clusterState));
    }

    @Override
    public int hashCode() {
        // Best effort. Only use cluster state version and master node id,
        // because cluster state doesn't implement  hashcode()
        return Objects.hash(
            clusterName,
            getVersion(clusterState),
            getMasterNodeId(clusterState),
            waitForTimedOut
        );
    }

    private static String getMasterNodeId(ClusterState clusterState) {
        if (clusterState == null) {
            return null;
        }
        DiscoveryNodes nodes = clusterState.nodes();
        if (nodes != null) {
            return nodes.getMasterNodeId();
        } else {
            return null;
        }
    }

    private static Long getVersion(ClusterState clusterState) {
        if (clusterState != null) {
            return clusterState.version();
        } else {
            return null;
        }
    }

}
