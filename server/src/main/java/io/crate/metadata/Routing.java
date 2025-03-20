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

package io.crate.metadata;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.discovery.MasterNotDiscoveredException;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.IntIndexedContainer;
import com.carrotsearch.hppc.cursors.IntCursor;

public class Routing implements Writeable {

    private final Map<String, Map<String, IntIndexedContainer>> locations;

    public Routing(Map<String, Map<String, IntIndexedContainer>> locations) {
        assert locations != null : "locations must not be null";
        assert assertLocationsAllTreeMap(locations) : "locations must be a TreeMap only and must contain only TreeMap's";
        this.locations = locations;
    }

    /**
     * @return a map with the locations in the following format: <p>
     * Map&lt;nodeName (string), <br>
     * &nbsp;&nbsp;&nbsp;&nbsp;Map&lt;indexName (string), List&lt;ShardId (int)&gt;&gt;&gt; <br>
     * </p>
     */
    public Map<String, Map<String, IntIndexedContainer>> locations() {
        return locations;
    }

    public boolean hasLocations() {
        return locations.size() > 0;
    }

    public Set<String> nodes() {
        return locations.keySet();
    }

    /**
     * get the number of shards in this routing for a node with given nodeId
     *
     * @return int &gt;= 0
     */
    public int numShards(String nodeId) {
        Map<String, IntIndexedContainer> indicesAndShards = locations.get(nodeId);
        if (indicesAndShards == null) {
            return 0;
        }
        int numShards = 0;
        for (IntIndexedContainer shardIds : indicesAndShards.values()) {
            numShards += shardIds.size();
        }
        return numShards;
    }

    /**
     * returns true if the routing contains shards for any table of the given node
     */
    public boolean containsShards(String nodeId) {
        Map<String, IntIndexedContainer> indicesAndShards = locations.get(nodeId);
        if (indicesAndShards == null) {
            return false;
        }
        for (IntIndexedContainer shardIds : indicesAndShards.values()) {
            if (!shardIds.isEmpty()) {
                return true;
            }
        }
        return false;
    }

    public boolean containsShards() {
        for (Map<String, IntIndexedContainer> indices : locations.values()) {
            for (IntIndexedContainer shards : indices.values()) {
                if (!shards.isEmpty()) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public String toString() {
        return "Routing{" +
               "locations=" + locations +
               '}';
    }

    public Routing(StreamInput in) throws IOException {
        int numLocations = in.readVInt();
        if (numLocations == 0) {
            locations = Map.of();
        } else {
            locations = new TreeMap<>();

            for (int i = 0; i < numLocations; i++) {
                String nodeId = in.readString();
                int numInner = in.readVInt();
                Map<String, IntIndexedContainer> shardsByIndex = new TreeMap<>();

                locations.put(nodeId, shardsByIndex);
                for (int j = 0; j < numInner; j++) {
                    String indexName = in.readString();
                    int numShards = in.readVInt();
                    IntArrayList shardIds = new IntArrayList(numShards);
                    for (int k = 0; k < numShards; k++) {
                        shardIds.add(in.readVInt());
                    }
                    shardsByIndex.put(indexName, shardIds);
                }
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(locations.size());
        for (Map.Entry<String, Map<String, IntIndexedContainer>> entry : locations.entrySet()) {
            out.writeString(entry.getKey());

            Map<String, IntIndexedContainer> shardsByIndex = entry.getValue();
            if (shardsByIndex == null) {
                out.writeVInt(0);
            } else {
                out.writeVInt(shardsByIndex.size());
                for (Map.Entry<String, IntIndexedContainer> innerEntry : shardsByIndex.entrySet()) {
                    out.writeString(innerEntry.getKey());
                    IntIndexedContainer shardIds = innerEntry.getValue();
                    if (shardIds == null || shardIds.size() == 0) {
                        out.writeVInt(0);
                    } else {
                        out.writeVInt(shardIds.size());
                        for (IntCursor shardId: shardIds) {
                            out.writeVInt(shardId.value);
                        }
                    }
                }
            }
        }
    }

    private boolean assertLocationsAllTreeMap(Map<String, Map<String, IntIndexedContainer>> locations) {
        if (locations.isEmpty()) {
            return true;
        }
        if (!(locations instanceof TreeMap) && locations.size() > 1) {
            return false;
        }
        for (Map<String, IntIndexedContainer> shardsByIndex : locations.values()) {
            if (shardsByIndex.size() > 1 && !(shardsByIndex instanceof TreeMap)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Return a routing for the given table on the given node id.
     */
    public static Routing forTableOnSingleNode(RelationName relationName, String nodeId) {
        Map<String, Map<String, IntIndexedContainer>> locations = new TreeMap<>();
        Map<String, IntIndexedContainer> tableLocation = new TreeMap<>();
        tableLocation.put(relationName.fqn(), IntArrayList.from(IntArrayList.EMPTY_ARRAY));
        locations.put(nodeId, tableLocation);
        return new Routing(locations);
    }

    public static Routing forTableOnAllNodes(RelationName relationName, DiscoveryNodes nodes) {
        TreeMap<String, Map<String, IntIndexedContainer>> indicesByNode = new TreeMap<>();
        Map<String, IntIndexedContainer> shardsByIndex = Collections.singletonMap(
            relationName.indexNameOrAlias(),
            IntArrayList.from(IntArrayList.EMPTY_ARRAY)
        );
        for (DiscoveryNode node : nodes) {
            indicesByNode.put(node.getId(), shardsByIndex);
        }
        return new Routing(indicesByNode);
    }

    public static Routing forMasterNode(RelationName relationName, ClusterState clusterState) {
        String masterNodeId = clusterState.nodes().getMasterNodeId();
        if (masterNodeId == null) {
            throw new MasterNotDiscoveredException();
        }
        return forTableOnSingleNode(relationName, masterNodeId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Routing other = (Routing) o;
        return locations.equals(other.locations);
    }

    @Override
    public int hashCode() {
        return locations.hashCode();
    }
}
