/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.types;

import com.google.common.base.Preconditions;
import io.crate.Streamer;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Arrays;

public class GeoPointType extends DataType<Double[]> implements Streamer<Double[]>, DataTypeFactory {

    public static final int ID = 13;
    public static final GeoPointType INSTANCE = new GeoPointType();
    private GeoPointType() {}

    @Override
    public int id() {
        return ID;
    }

    @Override
    public String getName() {
        return "geo_point";
    }

    @Override
    public Streamer<?> streamer() {
        return this;
    }

    @Override
    public Double[] value(Object value) {
        if (value == null) {
            return null;
        }
        Object[] values = (Object[])value;
        Preconditions.checkArgument(values.length == 2);
        return Arrays.copyOf(values, 2, Double[].class);
    }

    @Override
    public int compareValueTo(Double[] val1, Double[] val2) {
        if (val1 == null) {
            return -1;
        }
        if (val2 == null) {
            return 1;
        }
        assert val1.length == 2;
        assert val2.length == 2;

        // this is probably not really correct, but should be sufficient for the compareValueTo use case
        // (which is ordering and equality check)
        int latComp = Double.compare(val1[0], val2[0]);
        if (latComp != 0) {
            return latComp;
        }
        return Double.compare(val1[1], val2[1]);
    }

    @Override
    public DataType<?> create() {
        return INSTANCE;
    }

    @Override
    public Double[] readValueFrom(StreamInput in) throws IOException {
        if (in.readBoolean()) {
            return new Double[] {in.readDouble(), in.readDouble()};
        } else {
            return null;
        }
    }

    @Override
    public void writeValueTo(StreamOutput out, Object v) throws IOException {
        if (v == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            Double[] point = (Double[]) v;
            out.writeDouble(point[0]);
            out.writeDouble(point[1]);
        }
    }
}
