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

package io.crate.types;

import java.io.IOException;
import java.util.Map;
import java.util.function.Function;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.BytesRefs;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.shape.Shape;

import io.crate.Streamer;
import io.crate.execution.dml.GeoShapeIndexer;
import io.crate.execution.dml.ValueIndexer;
import io.crate.geo.GeoJSONUtils;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.types.geo.GeoShape;

public class GeoShapeType extends DataType<GeoShape> implements Streamer<GeoShape> {

    public static final int ID = 14;
    public static final GeoShapeType INSTANCE = new GeoShapeType();


    public static class Names {
        public static final String TREE_GEOHASH = "geohash";
        public static final String TREE_QUADTREE = "quadtree";
        public static final String TREE_LEGACY_QUADTREE = "legacyquadtree";
        public static final String TREE_BKD = "bkdtree";
    }


    private static final StorageSupport<GeoShape> STORAGE = new StorageSupport<>(false, false, null) {

        @Override
        public ValueIndexer<GeoShape> valueIndexer(RelationName table,
                                                   Reference ref,
                                                   Function<ColumnIdent, Reference> getRef) {
            return new GeoShapeIndexer(ref);
        }
    };

    private GeoShapeType() {
    }

    @Override
    public int id() {
        return ID;
    }

    @Override
    public Precedence precedence() {
        return Precedence.GEO_SHAPE;
    }

    @Override
    public String getName() {
        return "geo_shape";
    }

    @Override
    public Streamer<GeoShape> streamer() {
        return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public GeoShape implicitCast(Object value) throws IllegalArgumentException, ClassCastException {
        if (value == null) {
            return null;
        } else if (value instanceof String str) {
            return GeoJSONUtils.wkt2Map(str);
        } else if (value instanceof Point point) {
            return GeoJSONUtils.shape2Map(SpatialContext.GEO.getShapeFactory().pointXY(point.getX(), point.getY()));
        } else if (value instanceof Shape shape) {
            return GeoJSONUtils.shape2Map(shape);
        } else if (value instanceof Map<?, ?> map) {
            GeoJSONUtils.validateGeoJson(map);
            return (Map<String, Object>) value;
        } else {
            throw new ClassCastException("Can't cast '" + value + "' to " + getName());
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public GeoShape sanitizeValue(Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Map<?, ?> map) {
            GeoJSONUtils.validateGeoJson(map);
            return GeoJSONUtils.spatialShapeToGeoShape(GeoJSONUtils.map2Shape((Map<String, Object>) map));
        } else {
            return GeoJSONUtils.spatialShapeToGeoShape((Shape) value);
        }
    }

    @Override
    public int compare(GeoShape val1, GeoShape val2) {
        // TODO: compare without converting to shape
        Shape shape1 = GeoJSONUtils.map2Shape(val1);
        Shape shape2 = GeoJSONUtils.map2Shape(val2);
        return switch (shape1.relate(shape2)) {
            case WITHIN -> -1;
            case CONTAINS -> 1;
            default -> Double.compare(shape1.getArea(JtsSpatialContext.GEO), shape2.getArea(JtsSpatialContext.GEO));
        };
    }

    @Override
    public GeoShape readValueFrom(StreamInput in) throws IOException {
        return GeoShape.fromStream(in);
    }

    @Override
    public void writeValueTo(StreamOutput out, GeoShape v) throws IOException {
        GeoShape.toStream(out, v);
    }

    @Override
    public StorageSupport<GeoShape> storageSupport() {
        return STORAGE;
    }

    @Override
    public long valueBytes(GeoShape value) {
        if (value == null) {
            return RamUsageEstimator.NUM_BYTES_OBJECT_HEADER;
        }
        return value.ramBytesUsed();
    }
}
