package io.crate.metadata;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;
import java.util.*;

public class Routing implements Streamable {

    private Map<String, Map<String, List<Integer>>> locations;
    private volatile int numShards = -1;

    public static abstract class RoutingLocationVisitor {

        public boolean visitLocations(Map<String, Map<String, List<Integer>>> locations){
            return true;
        }

        public boolean visitNode(String nodeId, Map<String, List<Integer>> nodeRouting) {
            return true;
        }

        public boolean visitIndex(String nodeId, String index, List<Integer> shardIds) {
            return true;
        }

        public boolean visitShard(String nodeId, String index, Integer shardId) {
            return false;
        }
    }

    public void walkLocations(RoutingLocationVisitor visitor) {
        if (!visitor.visitLocations(locations)) {
            return;
        }
        for (Map.Entry<String, Map<String, List<Integer>>> location : locations.entrySet()) {
            if (!visitor.visitNode(location.getKey(), location.getValue())) {
                break;
            }
            for (Map.Entry<String, List<Integer>> entry : location.getValue().entrySet()) {
                if (!visitor.visitIndex(location.getKey(), entry.getKey(), entry.getValue())) {
                    break;
                }
                for (Integer shardId : entry.getValue()) {
                    //noinspection ConstantConditions
                    if (!visitor.visitShard(location.getKey(), entry.getKey(), shardId)) {
                        break;
                    }
                }
            }
        }
    }

    private Routing() {}

    public static Routing fromStream(StreamInput in) throws IOException {
        Routing routing = new Routing();
        routing.readFrom(in);
        return routing;
    }

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
        int count = 0;
        if (!locations.isEmpty()) {
            Map<String, List<Integer>> nodeRouting = locations.get(nodeId);
            if (nodeRouting != null) {
                for (List<Integer> shardIds : nodeRouting.values()) {
                    if (shardIds != null) {
                        count += shardIds.size();
                    }
                }
            }
        }
        return count;
    }

    /**
     * returns true if the routing contains shards for any table of the given node
     */
    public boolean containsShards(String nodeId) {
        if (locations.isEmpty()) return false;
        Map<String, List<Integer>> nodeRouting = locations.get(nodeId);
        if (nodeRouting == null) return false;

        for (Map.Entry<String, List<Integer>> tableEntry : nodeRouting.entrySet()) {
            if (tableEntry.getValue() != null && !tableEntry.getValue().isEmpty()) {
                return true;
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

    @Override
    public void readFrom(StreamInput in) throws IOException {
        int numLocations = in.readVInt();
        if (numLocations == 0) {
            locations = ImmutableMap.of();
        } else {
            locations = new TreeMap<>();

            String nodeId;
            int numInner;
            Map<String, List<Integer>> innerMap;
            for (int i = 0; i < numLocations; i++) {
                nodeId = in.readString();
                numInner = in.readVInt();
                innerMap = new TreeMap<>();

                locations.put(nodeId, innerMap);
                for (int j = 0; j < numInner; j++) {
                    String key = in.readString();
                    int numShards = in.readVInt();
                    List<Integer> shardIds = new ArrayList<>(numShards);
                    for (int k = 0; k < numShards; k++) {
                        shardIds.add(in.readVInt());
                    }
                    innerMap.put(key, shardIds);
                }
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(locations.size());
        for (Map.Entry<String, Map<String, List<Integer>>> entry : locations.entrySet()) {
            out.writeString(entry.getKey());

            if (entry.getValue() == null) {
                out.writeVInt(0);
            } else {
                out.writeVInt(entry.getValue().size());
                for (Map.Entry<String, List<Integer>> innerEntry : entry.getValue().entrySet()) {
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
        if (!(locations instanceof TreeMap)) {
            return false;
        }
        for (Map<String, List<Integer>> innerMap : locations.values()) {
            if (!(innerMap instanceof TreeMap)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Return a routing for the given table on the given node id.
     */
    public static Routing forTableOnNode(TableIdent tableIdent, String nodeId) {
        Map<String, Map<String, List<Integer>>> locations = new TreeMap<>();
        Map<String, List<Integer>> tableLocation = new TreeMap<>();
        tableLocation.put(tableIdent.fqn(), null);
        locations.put(nodeId, tableLocation);
        return new Routing(locations);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Routing routing = (Routing) o;
        return Objects.equals(numShards, routing.numShards) &&
                Objects.equals(locations, routing.locations);
    }

    @Override
    public int hashCode() {
        return Objects.hash(locations, numShards);
    }
}
