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

package org.elasticsearch.cluster.health;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

public final class ClusterIndexHealth implements Iterable<ClusterShardHealth>, Writeable {

    private final String index;
    private final int numberOfShards;
    private final int numberOfReplicas;
    private final int activeShards;
    private final int relocatingShards;
    private final int initializingShards;
    private final int unassignedShards;
    private final int activePrimaryShards;
    private final ClusterHealthStatus status;
    private final Map<Integer, ClusterShardHealth> shards;

    public ClusterIndexHealth(final IndexMetadata indexMetadata, final IndexRoutingTable indexRoutingTable) {
        this.index = indexMetadata.getIndex().getName();
        this.numberOfShards = indexMetadata.getNumberOfShards();
        this.numberOfReplicas = indexMetadata.getNumberOfReplicas();

        shards = new HashMap<>();
        for (IndexShardRoutingTable shardRoutingTable : indexRoutingTable) {
            int shardId = shardRoutingTable.shardId().id();
            shards.put(shardId, new ClusterShardHealth(shardId, shardRoutingTable));
        }

        // update the index status
        ClusterHealthStatus computeStatus = ClusterHealthStatus.GREEN;
        int computeActivePrimaryShards = 0;
        int computeActiveShards = 0;
        int computeRelocatingShards = 0;
        int computeInitializingShards = 0;
        int computeUnassignedShards = 0;
        for (ClusterShardHealth shardHealth : shards.values()) {
            if (shardHealth.isPrimaryActive()) {
                computeActivePrimaryShards++;
            }
            computeActiveShards += shardHealth.getActiveShards();
            computeRelocatingShards += shardHealth.getRelocatingShards();
            computeInitializingShards += shardHealth.getInitializingShards();
            computeUnassignedShards += shardHealth.getUnassignedShards();

            if (shardHealth.getStatus() == ClusterHealthStatus.RED) {
                computeStatus = ClusterHealthStatus.RED;
            } else if (shardHealth.getStatus() == ClusterHealthStatus.YELLOW && computeStatus != ClusterHealthStatus.RED) {
                // do not override an existing red
                computeStatus = ClusterHealthStatus.YELLOW;
            }
        }
        if (shards.isEmpty()) { // might be since none has been created yet (two phase index creation)
            computeStatus = ClusterHealthStatus.RED;
        }

        this.status = computeStatus;
        this.activePrimaryShards = computeActivePrimaryShards;
        this.activeShards = computeActiveShards;
        this.relocatingShards = computeRelocatingShards;
        this.initializingShards = computeInitializingShards;
        this.unassignedShards = computeUnassignedShards;
    }

    public ClusterIndexHealth(final StreamInput in) throws IOException {
        index = in.readString();
        numberOfShards = in.readVInt();
        numberOfReplicas = in.readVInt();
        activePrimaryShards = in.readVInt();
        activeShards = in.readVInt();
        relocatingShards = in.readVInt();
        initializingShards = in.readVInt();
        unassignedShards = in.readVInt();
        status = ClusterHealthStatus.fromValue(in.readByte());

        int size = in.readVInt();
        shards = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            ClusterShardHealth shardHealth = new ClusterShardHealth(in);
            shards.put(shardHealth.getShardId(), shardHealth);
        }
    }

    public String getIndex() {
        return index;
    }

    public int getNumberOfShards() {
        return numberOfShards;
    }

    public int getNumberOfReplicas() {
        return numberOfReplicas;
    }

    public int getActiveShards() {
        return activeShards;
    }

    public int getRelocatingShards() {
        return relocatingShards;
    }

    public int getActivePrimaryShards() {
        return activePrimaryShards;
    }

    public int getInitializingShards() {
        return initializingShards;
    }

    public int getUnassignedShards() {
        return unassignedShards;
    }

    public ClusterHealthStatus getStatus() {
        return status;
    }

    public Map<Integer, ClusterShardHealth> getShards() {
        return this.shards;
    }

    @Override
    public Iterator<ClusterShardHealth> iterator() {
        return shards.values().iterator();
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeString(index);
        out.writeVInt(numberOfShards);
        out.writeVInt(numberOfReplicas);
        out.writeVInt(activePrimaryShards);
        out.writeVInt(activeShards);
        out.writeVInt(relocatingShards);
        out.writeVInt(initializingShards);
        out.writeVInt(unassignedShards);
        out.writeByte(status.value());
        out.writeCollection(shards.values());
    }


    @Override
    public String toString() {
        return "ClusterIndexHealth{" +
                "index='" + index + '\'' +
                ", numberOfShards=" + numberOfShards +
                ", numberOfReplicas=" + numberOfReplicas +
                ", activeShards=" + activeShards +
                ", relocatingShards=" + relocatingShards +
                ", initializingShards=" + initializingShards +
                ", unassignedShards=" + unassignedShards +
                ", activePrimaryShards=" + activePrimaryShards +
                ", status=" + status +
                ", shards.size=" + (shards == null ? "null" : shards.size()) +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClusterIndexHealth that = (ClusterIndexHealth) o;
        return Objects.equals(index, that.index) &&
                numberOfShards == that.numberOfShards &&
                numberOfReplicas == that.numberOfReplicas &&
                activeShards == that.activeShards &&
                relocatingShards == that.relocatingShards &&
                initializingShards == that.initializingShards &&
                unassignedShards == that.unassignedShards &&
                activePrimaryShards == that.activePrimaryShards &&
                status == that.status &&
                Objects.equals(shards, that.shards);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, numberOfShards, numberOfReplicas, activeShards, relocatingShards, initializingShards, unassignedShards,
                activePrimaryShards, status, shards);
    }
}
