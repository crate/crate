package io.crate.metadata;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableSet;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;

public class Routing implements Streamable {

    private Map<String, Map<String, List<Integer>>> locations;
    private volatile int numShards = -1;
    private int jobSearchContextIdBase = -1;

    public Routing() {

    }

    public Routing(@Nullable Map<String, Map<String, List<Integer>>> locations) {
        assert assertLocationsAllTreeMap(locations) : "locations must be a TreeMap only and must contain only TreeMap's";
        this.locations = locations;
    }

    /**
     * @return a map with the locations in the following format: <p>
     *  Map< nodeName (string), <br />
     *  &nbsp;&nbsp;&nbsp;&nbsp;Map< indexName (string), Set<ShardId (int) > <br />
     *  </p>
     */
    @Nullable
    public Map<String, Map<String, List<Integer>>> locations() {
        return locations;
    }

    public boolean hasLocations() {
        return locations != null && locations.size() > 0;
    }

    public Set<String> nodes() {
        if (hasLocations()) {
            return locations.keySet();
        }
        return ImmutableSet.of();
    }

    /**
     * get the number of shards in this routing for a node with given nodeId
     * @return int >= 0
     */
    public int numShards(String nodeId) {
        int count = 0;
        if (hasLocations()) {
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
     * get the number of shards in this routing
     * @return int >= 0
     */
    public int numShards() {
        if (numShards == -1) {
            int count = 0;
            if (hasLocations()) {
                for (Map<String, List<Integer>> nodeRouting : locations.values()) {
                    if (nodeRouting != null) {
                        for (List<Integer> shardIds : nodeRouting.values()) {
                            if (shardIds != null) {
                                count += shardIds.size();
                            }
                        }
                    }
                }
            }
            numShards = count;
        }

        return numShards;
    }

    /**
     * returns true if the routing contains shards for any table of the given node
     */
    public boolean containsShards(String nodeId) {
        if (!hasLocations()) return false;
        Map<String, List<Integer>> nodeRouting = locations.get(nodeId);
        if (nodeRouting == null) return false;

        for (Map.Entry<String, List<Integer>> tableEntry : nodeRouting.entrySet()) {
            if (tableEntry.getValue() != null && !tableEntry.getValue().isEmpty()) {
                return true;
            }
        }
        return false;
    }

    public void jobSearchContextIdBase(int jobSearchContextIdBase) {
        this.jobSearchContextIdBase = jobSearchContextIdBase;
    }

    public int jobSearchContextIdBase() {
        return jobSearchContextIdBase;
    }

    @Override
    public String toString() {
        MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this);
        if (hasLocations()) {
            helper.add("locations", locations);
        }
        helper.add("jobSearchContextIdBase", jobSearchContextIdBase);
        return helper.toString();

    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        int numLocations = in.readVInt();
        if (numLocations > 0) {
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
                    for (int k = 0; k<numShards;k++){
                        shardIds.add(in.readVInt());
                    }
                    innerMap.put(key, shardIds);
                }
            }
        }
        if (in.readBoolean()) {
            jobSearchContextIdBase = in.readVInt();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (hasLocations()) {
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
        } else {
            out.writeVInt(0);
        }
        if (jobSearchContextIdBase > -1) {
            out.writeBoolean(true);
            out.writeVInt(jobSearchContextIdBase);
        } else {
            out.writeBoolean(false);
        }
    }

    private boolean assertLocationsAllTreeMap(@Nullable Map<String, Map<String, List<Integer>>> locations) {
        if (locations != null) {
            if (!(locations instanceof TreeMap)) {
                return false;
            }
            for (Map<String, List<Integer>> innerMap : locations.values()) {
                if (!(innerMap instanceof TreeMap)) {
                    return false;
                }
            }
        }
        return true;
    }
}
