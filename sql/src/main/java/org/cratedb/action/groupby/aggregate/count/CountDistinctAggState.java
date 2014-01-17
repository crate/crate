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

package org.cratedb.action.groupby.aggregate.count;

import org.cratedb.action.groupby.aggregate.AggState;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Set;

public class CountDistinctAggState extends AggState<CountDistinctAggState> {

    // TODO: we have a limit of Integer.MAX_VALUE for counts due to seenValues.size()
    public Set<Object> seenValues;
    Long value;

    @Override
    public Object value() {
        return value;
    }

    @Override
    public void reduce(CountDistinctAggState other) {
    }

    @Override
    public void terminatePartial() {
        value = ((Number)seenValues.size()).longValue();
    }

    @Override
    public int compareTo(CountDistinctAggState o) {
        return Integer.compare(value == null ? seenValues.size() : value.intValue(), (Integer) o.value());
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        if (in.readBoolean()) {
            value = in.readVLong();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (value != null) {
            out.writeBoolean(true);
            out.writeVLong(value);
        } else {
            out.writeBoolean(false);
        }
    }

    @Override
    public void setSeenValuesRef(Set<Object> seenValues) {
        this.seenValues = seenValues;
    }
}
