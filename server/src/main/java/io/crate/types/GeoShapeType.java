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
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.function.Function;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.ByteBufferStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.BytesRefs;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.Shape;

import io.crate.Streamer;
import io.crate.execution.dml.GeoShapeIndexer;
import io.crate.execution.dml.ValueIndexer;
import io.crate.expression.reference.doc.lucene.SourceParser;
import io.crate.geo.GeoJSONUtils;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;

public class GeoShapeType extends DataType<Map<String, Object>> implements Streamer<Map<String, Object>> {

    public static final int ID = 14;
    public static final GeoShapeType INSTANCE = new GeoShapeType();


    public static class Names {
        public static final String TREE_GEOHASH = "geohash";
        public static final String TREE_QUADTREE = "quadtree";
        public static final String TREE_LEGACY_QUADTREE = "legacyquadtree";
        public static final String TREE_BKD = "bkdtree";
    }


    private static final StorageSupport<Map<String, Object>> STORAGE = new StorageSupport<>(false, false, null) {

        @Override
        public ValueIndexer<Map<String, Object>> valueIndexer(RelationName table,
                                                              Reference ref,
                                                              Function<ColumnIdent, Reference> getRef) {
            return new GeoShapeIndexer(ref);
        }

        @Override
        public Map<String, Object> decode(ColumnIdent column, SourceParser sourceParser, Version tableVersion, byte[] bytes) {
            try (StreamInput in = new ByteBufferStreamInput(ByteBuffer.wrap(bytes))) {
                in.setVersion(tableVersion);
                return GeoShapeType.INSTANCE.streamer().readValueFrom(in);
            } catch (IOException ee) {
                throw new UncheckedIOException(ee);
            }
        }

        @Override
        public boolean retrieveFromStoredFields() {
            // there's a difference in precision between the stored value and the dv value
            // so when we're retrieving for display or exact comparisons, go to the stored
            // value
            return true;
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
    public Streamer<Map<String, Object>> streamer() {
        return this;
    }

    @Override
    public Map<String, Object> implicitCast(Object value) throws IllegalArgumentException, ClassCastException {
        if (value == null) {
            return null;
        } else if (value instanceof String) {
            return GeoJSONUtils.wkt2Map(BytesRefs.toString(value));
        } else if (value instanceof Point point) {
            return GeoJSONUtils.shape2Map(SpatialContext.GEO.getShapeFactory().pointXY(point.getX(), point.getY()));
        } else if (value instanceof Shape shape) {
            return GeoJSONUtils.shape2Map(shape);
        } else if (value instanceof Map<?, ?> map) {
            return GeoJSONUtils.sanitizeMap(map);
        } else {
            throw new ClassCastException("Can't cast '" + value + "' to " + getName());
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, Object> sanitizeValue(Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Map<?, ?> map) {
            return GeoJSONUtils.sanitizeMap(map);
        } else {
            return GeoJSONUtils.shape2Map((Shape) value);
        }
    }

    @Override
    public int compare(Map<String, Object> val1, Map<String, Object> val2) {
        return GeoJSONUtils.compare(val1, val2);
    }

    @Override
    public Map<String, Object> readValueFrom(StreamInput in) throws IOException {
        return in.readMap();
    }

    @Override
    public void writeValueTo(StreamOutput out, Map<String, Object> v) throws IOException {
        out.writeMap(v);
    }

    @Override
    public StorageSupport<Map<String, Object>> storageSupport() {
        return STORAGE;
    }

    private static long sizeOf(Object object) {
        // Not Using RamUsageEstimator.sizeOfMap because it is limited to depth 1
        // Coordinates can be arrays/lists with 4 levels,
        // and in geometry collections the whole thing is within a list of objects
        //
        // depending on the source of the GeoJSON it is either using lists or double arrays
        return switch (object) {
            case Map<?, ?> map -> sizeOf(map);
            case Collection<?> collection -> {
                long bytes = RamUsageEstimator.shallowSizeOf(collection);
                for (var item : collection) {
                    bytes += sizeOf(item);
                }
                yield bytes;
            }
            case double[][][][] multiPolygon -> {
                long bytes = RamUsageEstimator.NUM_BYTES_ARRAY_HEADER;
                for (double[][][] polygon : multiPolygon) {
                    bytes += sizeOf(polygon);
                }
                yield bytes;
            }
            case double[][][] polygon -> {
                long bytes = RamUsageEstimator.NUM_BYTES_ARRAY_HEADER;
                for (double[][] lineRing : polygon) {
                    bytes += sizeOf(lineRing);
                }
                yield bytes;
            }
            case double[][] lineRing -> {
                long bytes = RamUsageEstimator.NUM_BYTES_ARRAY_HEADER;
                for (double[] points : lineRing) {
                    bytes += RamUsageEstimator.sizeOf(points);
                }
                yield bytes;
            }
            default -> RamUsageEstimator.sizeOfObject(object);
        };
    }

    private static long sizeOf(Map<?, ?> map) {
        long bytes = RamUsageEstimator.shallowSizeOf(map);
        long entrySize = -1;
        for (var entry : map.entrySet()) {
            if (entrySize == -1) {
                entrySize = RamUsageEstimator.shallowSizeOf(entry);
            }
            bytes += entrySize;

            // Keys should be strings, no problem with depth here:
            Object key = entry.getKey();
            bytes += RamUsageEstimator.sizeOfObject(key);

            Object value = entry.getValue();
            bytes += sizeOf(value);
        }
        return bytes;
    }

    @Override
    public long valueBytes(Map<String, Object> map) {
        if (map == null) {
            return RamUsageEstimator.NUM_BYTES_OBJECT_HEADER;
        }
        return RamUsageEstimator.alignObjectSize(sizeOf(map));
    }
}
