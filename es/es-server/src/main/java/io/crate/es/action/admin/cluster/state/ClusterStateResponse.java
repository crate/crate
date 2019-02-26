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

package io.crate.es.action.admin.cluster.state;

import io.crate.es.action.ActionResponse;
import io.crate.es.cluster.ClusterName;
import io.crate.es.cluster.ClusterState;
import io.crate.es.common.io.stream.StreamInput;
import io.crate.es.common.io.stream.StreamOutput;
import io.crate.es.common.unit.ByteSizeValue;

import java.io.IOException;

/**
 * The response for getting the cluster state.
 */
public class ClusterStateResponse extends ActionResponse {

    private ClusterName clusterName;
    private ClusterState clusterState;
    // the total compressed size of the full cluster state, not just
    // the parts included in this response
    private ByteSizeValue totalCompressedSize;

    public ClusterStateResponse() {
    }

    public ClusterStateResponse(ClusterName clusterName, ClusterState clusterState, long sizeInBytes) {
        this.clusterName = clusterName;
        this.clusterState = clusterState;
        this.totalCompressedSize = new ByteSizeValue(sizeInBytes);
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
     * The total compressed size of the full cluster state, not just the parts
     * returned by {@link #getState()}.  The total compressed size is the size
     * of the cluster state as it would be transmitted over the network during
     * intra-node communication.
     */
    public ByteSizeValue getTotalCompressedSize() {
        return totalCompressedSize;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        clusterName = new ClusterName(in);
        clusterState = ClusterState.readFrom(in, null);
        totalCompressedSize = new ByteSizeValue(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        clusterName.writeTo(out);
        clusterState.writeTo(out);
        totalCompressedSize.writeTo(out);
    }
}
