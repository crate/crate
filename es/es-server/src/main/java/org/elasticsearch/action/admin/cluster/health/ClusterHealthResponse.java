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

package org.elasticsearch.action.admin.cluster.health;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.health.ClusterIndexHealth;
import org.elasticsearch.cluster.health.ClusterStateHealth;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import io.crate.common.unit.TimeValue;
import org.elasticsearch.transport.TransportResponse;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class ClusterHealthResponse extends TransportResponse {

    private final String clusterName;
    private final int numberOfPendingTasks;
    private final int numberOfInFlightFetch;
    private final int delayedUnassignedShards;
    private final TimeValue taskMaxWaitingTime;
    private final ClusterStateHealth clusterStateHealth;

    private ClusterHealthStatus clusterHealthStatus;
    private boolean timedOut = false;

    public ClusterHealthResponse(String clusterName, String[] concreteIndices, ClusterState clusterState, int numberOfPendingTasks,
                                 int numberOfInFlightFetch, int delayedUnassignedShards, TimeValue taskMaxWaitingTime) {
        this.clusterName = clusterName;
        this.numberOfPendingTasks = numberOfPendingTasks;
        this.numberOfInFlightFetch = numberOfInFlightFetch;
        this.delayedUnassignedShards = delayedUnassignedShards;
        this.taskMaxWaitingTime = taskMaxWaitingTime;
        this.clusterStateHealth = new ClusterStateHealth(clusterState, concreteIndices);
        this.clusterHealthStatus = clusterStateHealth.getStatus();
    }

    public String getClusterName() {
        return clusterName;
    }

    public int getActiveShards() {
        return clusterStateHealth.getActiveShards();
    }

    public int getRelocatingShards() {
        return clusterStateHealth.getRelocatingShards();
    }

    public int getActivePrimaryShards() {
        return clusterStateHealth.getActivePrimaryShards();
    }

    public int getInitializingShards() {
        return clusterStateHealth.getInitializingShards();
    }

    public int getUnassignedShards() {
        return clusterStateHealth.getUnassignedShards();
    }

    public int getNumberOfNodes() {
        return clusterStateHealth.getNumberOfNodes();
    }

    public int getNumberOfDataNodes() {
        return clusterStateHealth.getNumberOfDataNodes();
    }

    public int getNumberOfPendingTasks() {
        return this.numberOfPendingTasks;
    }

    public int getNumberOfInFlightFetch() {
        return this.numberOfInFlightFetch;
    }

    /**
     * The number of unassigned shards that are currently being delayed (for example,
     * due to node leaving the cluster and waiting for a timeout for the node to come
     * back in order to allocate the shards back to it).
     */
    public int getDelayedUnassignedShards() {
        return this.delayedUnassignedShards;
    }

    /**
     * {@code true} if the waitForXXX has timeout out and did not match.
     */
    public boolean isTimedOut() {
        return this.timedOut;
    }

    public void setTimedOut(boolean timedOut) {
        this.timedOut = timedOut;
    }

    public ClusterHealthStatus getStatus() {
        return clusterHealthStatus;
    }

    /**
     * Allows to explicitly override the derived cluster health status.
     *
     * @param status The override status. Must not be null.
     */
    public void setStatus(ClusterHealthStatus status) {
        if (status == null) {
            throw new IllegalArgumentException("'status' must not be null");
        }
        this.clusterHealthStatus = status;
    }

    public Map<String, ClusterIndexHealth> getIndices() {
        return clusterStateHealth.getIndices();
    }

    /**
     *
     * @return The maximum wait time of all tasks in the queue
     */
    public TimeValue getTaskMaxWaitingTime() {
        return taskMaxWaitingTime;
    }

    /**
     * The percentage of active shards, should be 100% in a green system
     */
    public double getActiveShardsPercent() {
        return clusterStateHealth.getActiveShardsPercent();
    }

    public ClusterHealthResponse(StreamInput in) throws IOException {
        clusterName = in.readString();
        clusterHealthStatus = ClusterHealthStatus.fromValue(in.readByte());
        clusterStateHealth = new ClusterStateHealth(in);
        numberOfPendingTasks = in.readInt();
        timedOut = in.readBoolean();
        numberOfInFlightFetch = in.readInt();
        delayedUnassignedShards = in.readInt();
        taskMaxWaitingTime = in.readTimeValue();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(clusterName);
        out.writeByte(clusterHealthStatus.value());
        clusterStateHealth.writeTo(out);
        out.writeInt(numberOfPendingTasks);
        out.writeBoolean(timedOut);
        out.writeInt(numberOfInFlightFetch);
        out.writeInt(delayedUnassignedShards);
        out.writeTimeValue(taskMaxWaitingTime);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClusterHealthResponse that = (ClusterHealthResponse) o;
        return Objects.equals(clusterName, that.clusterName) &&
                numberOfPendingTasks == that.numberOfPendingTasks &&
                numberOfInFlightFetch == that.numberOfInFlightFetch &&
                delayedUnassignedShards == that.delayedUnassignedShards &&
                Objects.equals(taskMaxWaitingTime, that.taskMaxWaitingTime) &&
                timedOut == that.timedOut &&
                Objects.equals(clusterStateHealth, that.clusterStateHealth) &&
                clusterHealthStatus == that.clusterHealthStatus;
    }

    @Override
    public int hashCode() {
        return Objects.hash(clusterName, numberOfPendingTasks, numberOfInFlightFetch, delayedUnassignedShards, taskMaxWaitingTime,
                timedOut, clusterStateHealth, clusterHealthStatus);
    }
}
