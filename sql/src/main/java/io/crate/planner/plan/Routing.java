/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.planner.plan;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class Routing implements Streamable {

    public Routing() {

    }

    private Map<String, Map<String, Integer>> locations;

    public Routing(Map<String, Map<String, Integer>> locations) {
        this.locations = locations;
    }

    public Map<String, Map<String, Integer>> locations() {
        return locations;
    }

    public boolean hasLocations() {
        return locations != null && locations().size() > 0;
    }

    public Set<String> nodes() {
        if (hasLocations()) {
            return locations.keySet();
        }
        return ImmutableSet.of();
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        int numLocations = in.readVInt();
        locations = new HashMap<>(numLocations);

        String nodeId;
        int numInner;
        Map<String, Integer> innerMap;
        for (int i = 0; i < numLocations; i++) {
            nodeId = in.readString();
            numInner = in.readVInt();
            innerMap = new HashMap<>(numInner);

            locations.put(nodeId, innerMap);
            for (int j = 0; j < numInner; j++) {
                innerMap.put(in.readString(), in.readVInt());
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(locations.size());

        for (Map.Entry<String, Map<String, Integer>> entry : locations.entrySet()) {
            out.writeString(entry.getKey());

            if (entry.getValue() == null) {
                out.writeVInt(0);
            } else {

                out.writeVInt(entry.getValue().size());
                for (Map.Entry<String, Integer> innerEntry : entry.getValue().entrySet()) {
                    out.writeString(innerEntry.getKey());
                    out.writeVInt(innerEntry.getValue());
                }
            }
        }
    }
}
