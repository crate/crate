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
import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.shape.Point;
import io.crate.Streamer;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.BytesRefs;

import java.io.IOException;
import java.text.ParseException;
import java.util.Arrays;
import java.util.List;

public class GeoPointType extends DataType<Double[]> implements Streamer<Double[]>, DataTypeFactory, FixedWithType {

    public static final int ID = 13;
    public static final GeoPointType INSTANCE = new GeoPointType();
    private GeoPointType() {}

    private static final SpatialContext SPATIAL_CONTEXT = SpatialContext.GEO;

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
        if (value instanceof BytesRef) {
            return pointFromString(BytesRefs.toString(value));
        }
        if (value instanceof String) {
            return pointFromString((String) value);
        }
        if (value instanceof List)  {
            List values = (List) value;
            Preconditions.checkArgument(values.size() == 2,
                    "The value of a GeoPoint must be a double array with 2 items, not %s", values.size());
            return new Double[] { (Double) values.get(0), (Double) values.get(1) };
        }
        Object[] values = (Object[])value;
        Preconditions.checkArgument(values.length == 2,
                "The value of a GeoPoint must be a double array with 2 items, not %s", values.length);
        return Arrays.copyOf(values, 2, Double[].class);
    }

    private static Double[] pointFromString(String value) {
        try {
            Point point = (Point)SPATIAL_CONTEXT.readShapeFromWkt(value);
            return new Double[] {point.getX(), point.getY()};
        } catch (ParseException e) {
            throw new IllegalArgumentException(String.format(
                    "Cannot convert \"%s\" to geo_point", value), e);
        }
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

    @Override
    public int fixedSize() {
        return 40; // 2x double + array overhead
    }
}
