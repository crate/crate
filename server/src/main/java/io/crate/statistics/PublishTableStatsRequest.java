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

package io.crate.statistics;

import io.crate.metadata.RelationName;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public final class PublishTableStatsRequest extends TransportRequest {

    private final Map<RelationName, Stats> statsByRelation;

    public PublishTableStatsRequest(Map<RelationName, Stats> statsByRelation) {
        this.statsByRelation = statsByRelation;
    }

    public PublishTableStatsRequest(StreamInput in) throws IOException {
        int numRelations = in.readVInt();
        statsByRelation = new HashMap<>();
        for (int i = 0; i < numRelations; i++) {
            statsByRelation.put(new RelationName(in), new Stats(in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(statsByRelation.size());
        for (var entry : statsByRelation.entrySet()) {
            entry.getKey().writeTo(out);
            entry.getValue().writeTo(out);
        }
    }

    public Map<RelationName, Stats> tableStats() {
        return statsByRelation;
    }
}
