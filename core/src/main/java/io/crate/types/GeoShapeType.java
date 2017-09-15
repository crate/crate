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

import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.shape.Shape;
import io.crate.Streamer;
import io.crate.geo.GeoJSONUtils;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.BytesRefs;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;

public class GeoShapeType extends DataType<Map<String, Object>> implements Streamer<Map<String, Object>> {

    public static final int ID = Precedence.GeoShapeType;
    public static final GeoShapeType INSTANCE = new GeoShapeType();

    private GeoShapeType() {
    }

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
    public Map<String, Object> value(Object value) throws IllegalArgumentException, ClassCastException {
        if (value == null) {
            return null;
        }
        try {
            if (value instanceof BytesRef || value instanceof String) {
                return GeoJSONUtils.wkt2Map(BytesRefs.toString(value));
            }
            if (value instanceof Shape) {
                return GeoJSONUtils.shape2Map((Shape) value);
            }
            if (value instanceof Map) {
                GeoJSONUtils.validateGeoJson((Map) value);
                return (Map<String, Object>) value;
            }
        } catch (Throwable e) {
            throw new IllegalArgumentException(invalidMsg(value), e);
        }
        throw new IllegalArgumentException(invalidMsg(value));
    }

    private String invalidMsg(Object value) {
        return String.format(Locale.ENGLISH, "Cannot convert \"%s\" to geo_shape", value);
    }


    @Override
    public int compareValueTo(Map<String, Object> val1, Map<String, Object> val2) {
        // TODO: compare without converting to shape
        Shape shape1 = GeoJSONUtils.map2Shape(val1);
        Shape shape2 = GeoJSONUtils.map2Shape(val2);
        switch (shape1.relate(shape2)) {
            case WITHIN:
                return -1;
            case CONTAINS:
                return 1;
            default:
                return Double.compare(shape1.getArea(JtsSpatialContext.GEO), shape2.getArea(JtsSpatialContext.GEO));
        }
    }

    @Override
    public Map<String, Object> readValueFrom(StreamInput in) throws IOException {
        return in.readMap();
    }

    @Override
    public void writeValueTo(StreamOutput out, Object v) throws IOException {
        out.writeMap((Map<String, Object>) v);
    }
}
