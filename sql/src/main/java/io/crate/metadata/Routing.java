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

package io.crate.metadata;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import io.crate.core.collections.TreeMapBuilder;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class Routing implements Writeable {

    private Map<String, Map<String, List<Integer>>> locations;

    public Routing(Map<String, Map<String, List<Integer>>> locations) {
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
    public Map<String, Map<String, List<Integer>>> locations() {
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
        Map<String, List<Integer>> indicesAndShards = locations.get(nodeId);
        if (indicesAndShards == null) {
            return 0;
        }
        int numShards = 0;
        for (List<Integer> shardIds : indicesAndShards.values()) {
            numShards += shardIds.size();
        }
        return numShards;
    }

    /**
     * returns true if the routing contains shards for any table of the given node
     */
    public boolean containsShards(String nodeId) {
        Map<String, List<Integer>> indicesAndShards = locations.get(nodeId);
        if (indicesAndShards == null) {
            return false;
        }
        for (List<Integer> shardIds : indicesAndShards.values()) {
            if (!shardIds.isEmpty()) {
                return true;
            }
        }
        return false;
    }

    public boolean containsShards() {
        for (Map<String, List<Integer>> indices : locations.values()) {
            for (List<Integer> shards : indices.values()) {
                if (!shards.isEmpty()) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public String toString() {
        MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this);
        helper.add("locations", locations);
        return helper.toString();

    }

    public Routing(StreamInput in) throws IOException {
        int numLocations = in.readVInt();
        if (numLocations == 0) {
            locations = ImmutableMap.of();
        } else {
            locations = new TreeMap<>();

            for (int i = 0; i < numLocations; i++) {
                String nodeId = in.readString();
                int numInner = in.readVInt();
                Map<String, List<Integer>> shardsByIndex = new TreeMap<>();

                locations.put(nodeId, shardsByIndex);
                for (int j = 0; j < numInner; j++) {
                    String indexName = in.readString();
                    int numShards = in.readVInt();
                    List<Integer> shardIds = new ArrayList<>(numShards);
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
        for (Map.Entry<String, Map<String, List<Integer>>> entry : locations.entrySet()) {
            out.writeString(entry.getKey());

            Map<String, List<Integer>> shardsByIndex = entry.getValue();
            if (shardsByIndex == null) {
                out.writeVInt(0);
            } else {
                out.writeVInt(shardsByIndex.size());
                for (Map.Entry<String, List<Integer>> innerEntry : shardsByIndex.entrySet()) {
                    out.writeString(innerEntry.getKey());
                    List<Integer> shardIds = innerEntry.getValue();
                    if (shardIds == null || shardIds.size() == 0) {
                        out.writeVInt(0);
                    } else {
                        out.writeVInt(shardIds.size());
                        for (Integer shardId : shardIds) {
                            out.writeVInt(shardId);
                        }
                    }
                }
            }
        }
    }

    private boolean assertLocationsAllTreeMap(Map<String, Map<String, List<Integer>>> locations) {
        if (locations.isEmpty()) {
            return true;
        }
        if (!(locations instanceof TreeMap) && locations.size() > 1) {
            return false;
        }
        for (Map<String, List<Integer>> innerMap : locations.values()) {
            if (innerMap.size() > 1 && !(innerMap instanceof TreeMap)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Return a routing for the given table on the given node id.
     */
    public static Routing forTableOnSingleNode(TableIdent tableIdent, String nodeId) {
        Map<String, Map<String, List<Integer>>> locations = new TreeMap<>();
        Map<String, List<Integer>> tableLocation = new TreeMap<>();
        tableLocation.put(tableIdent.fqn(), Collections.emptyList());
        locations.put(nodeId, tableLocation);
        return new Routing(locations);
    }

    public static Routing forTableOnAllNodes(TableIdent tableIdent, DiscoveryNodes nodes) {
        TreeMapBuilder<String, Map<String, List<Integer>>> nodesMapBuilder = TreeMapBuilder.newMapBuilder();
        Map<String, List<Integer>> tableMap = TreeMapBuilder.<String, List<Integer>>newMapBuilder()
            .put(tableIdent.fqn(), Collections.emptyList()).map();
        for (DiscoveryNode node : nodes) {
            nodesMapBuilder.put(node.getId(), tableMap);
        }
        return new Routing(nodesMapBuilder.map());
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
