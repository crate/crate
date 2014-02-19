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

package org.cratedb.action.groupby.aggregate.avg;

import org.cratedb.action.groupby.aggregate.AggState;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class AvgAggState extends AggState<AvgAggState> {

    private double sum = 0;
    private long count = 0;

    @Override
    public Object value() {
        if (count > 0) {
            return sum/count;
        } else {
            return null;
        }
    }

    @Override
    public void reduce(AvgAggState other) {
        if (other != null) {
            sum += other.sum;
            count += other.count;
        }
    }

    public void add(Object otherValue) {
        sum += ((Number)otherValue).doubleValue();
        count++;
    }

    @Override
    public int compareTo(AvgAggState o) {
        if (o==null) { return 1;}
        else {
            Double thisValue = (Double)value();
            Double other = (Double)o.value();
            return thisValue.compareTo(other);
        }
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        sum = in.readDouble();
        count = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeDouble(sum);
        out.writeVLong(count);
    }
}
