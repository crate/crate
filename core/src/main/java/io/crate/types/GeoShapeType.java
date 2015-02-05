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

import com.spatial4j.core.context.jts.JtsSpatialContext;
import com.spatial4j.core.io.BinaryCodec;
import com.spatial4j.core.shape.Shape;
import io.crate.Streamer;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.BytesRefs;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.text.ParseException;

public class GeoShapeType extends DataType<Shape> implements Streamer<Shape>, DataTypeFactory {

    public static final int ID = 14;
    public static final GeoShapeType INSTANCE = new GeoShapeType();

    private GeoShapeType() {}
    private final static BinaryCodec BINARY_CODEC = JtsSpatialContext.GEO.getBinaryCodec();

    @Override
    public int id() {
        return ID;
    }

    @Override
    public String getName() {
        return "geo_shape";
    }

    @Override
    public Streamer<?> streamer() {
        return this;
    }

    @Override
    public Shape value(Object value) throws IllegalArgumentException, ClassCastException {
        if (value == null) {
            return null;
        }
        if (value instanceof BytesRef) {
            return shapeFromString(BytesRefs.toString(value));
        }
        if (value instanceof String) {
            return shapeFromString((String) value);
        }
        if (value instanceof Shape) {
            return (Shape) value;
        }
        throw new IllegalArgumentException(String.format("Cannot convert \"%s\" to geo_shape", value));
    }

    @Override
    public int compareValueTo(Shape val1, Shape val2) {
        switch (val1.relate(val2)) {
            case WITHIN:
                return -1;
            case CONTAINS:
                return 1;
            default:
                return Double.compare(val1.getArea(JtsSpatialContext.GEO), val2.getArea(JtsSpatialContext.GEO));
        }
    }

    private Shape shapeFromString(String value) {
        try {
            return JtsSpatialContext.GEO.readShapeFromWkt(value);
        } catch (ParseException e) {
            throw new IllegalArgumentException(String.format(
                    "Cannot convert \"%s\" to geo_shape", value), e);
        }
    }

    @Override
    public DataType<?> create() {
        return INSTANCE;
    }

    @Override
    public Shape readValueFrom(StreamInput in) throws IOException {
        return BINARY_CODEC.readShape(new DataInputStream(in));
    }

    @Override
    public void writeValueTo(StreamOutput out, Object v) throws IOException {
        BINARY_CODEC.writeShape(new DataOutputStream(out), (Shape) v);
    }
}
