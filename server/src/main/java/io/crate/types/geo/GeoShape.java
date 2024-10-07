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

package io.crate.types.geo;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.locationtech.spatial4j.shape.Shape;

/**
 * https://datatracker.ietf.org/doc/html/rfc7946#section-3.1.6
 **/
public sealed interface GeoShape extends Accountable, Comparable<GeoShape> permits
    GeoShape.Point,
    GeoShape.LineString,
    GeoShape.Polygon,
    GeoShape.MultiPolygon,
    GeoShape.GeometryCollection {

    public static class Types {
        public static final String POINT = "Point";
        public static final String MULTI_POINT = "MultiPoint";
        public static final String LINE_STRING = "LineString";
        public static final String MULTI_LINE_STRING = "MultiLineString";
        public static final String POLYGON = "Polygon";
        public static final String MULTI_POLYGON = "MultiPolygon";
        public static final String GEOMETRY_COLLECTION = "GeometryCollection";
    }

    public static GeoShape of(String wkt) {
        throw new UnsupportedOperationException("NYI");
    }

    public static GeoShape of(Map<String, Object> geoJSON) {
        throw new UnsupportedOperationException("NYI");
    }

    default Map<String, Object> toGeoJSON() {
        throw new UnsupportedOperationException("NYI");
    }

    default Shape toShape() {
        throw new UnsupportedOperationException("NYI");
    }

    default Object toLuceneShape() {
        throw new UnsupportedOperationException("NYI");
    }

    public static GeoShape fromStream(StreamInput in) throws IOException {
        short type = in.readShort();
        // TODO:
        return switch (type) {
            case 0 -> new Point(in.readDoubleArray());
            default -> throw new IllegalArgumentException("Invalid shape type: " + type);
        };
    }

    public static void toStream(StreamOutput out, GeoShape value) throws IOException {
        switch (value) {
            case Point point -> {
                out.writeShort((short) 0);
                out.writeDoubleArray(point.xy());
            }
            case LineString lineString -> {
                out.writeShort((short) 1);
                double[][] points = lineString.points();
                out.writeVInt(points.length);
                for (double[] point : lineString.points()) {
                    out.writeDoubleArray(point);
                }
            }
            case Polygon polygon -> {
                out.writeShort((short) 2);
                double[][][] rings = polygon.linearRings();
                out.writeVInt(rings.length);
                for (double[][] ring : rings) {
                    out.writeVInt(ring.length);
                    for (double[] point : ring) {
                        out.writeDoubleArray(point);
                    }
                }
            }
            case MultiPolygon multiPolygon -> {
                out.writeShort((short) 3);
                double[][][][] polygons = multiPolygon.polygons();
                out.writeVInt(polygons.length);
                for (double[][][] polygon : polygons) {
                    out.writeVInt(polygon.length);
                    for (double[][] ring : polygon) {
                        out.writeVInt(ring.length);
                        for (double[] point : ring) {
                            out.writeDoubleArray(point);
                        }
                    }
                }
            }
            case GeometryCollection geometryCollection -> {
                out.writeShort((short) 4);
                List<GeoShape> shapes = geometryCollection.shapes();
                out.writeVInt(shapes.size());
                for (var shape : shapes) {
                    toStream(out, shape);
                }
            }
        };
    }

    public static record Point(double[] xy) implements GeoShape {

        @Override
        public long ramBytesUsed() {
            return RamUsageEstimator.sizeOf(xy);
        }

        @Override
        public int compareTo(GeoShape o) {
            return 0;
        }
    }

    public static record LineString(double[][] points) implements GeoShape {

        @Override
        public long ramBytesUsed() {
            long bytes = 0;
            for (double[] point : points) {
                bytes += RamUsageEstimator.sizeOf(point);
            }
            return bytes;
        }

        @Override
        public int compareTo(GeoShape o) {
            return 0;
        }
    }

    public static record Polygon(double[][][] linearRings) implements GeoShape {

        @Override
        public long ramBytesUsed() {
            long bytes = 0;
            for (double[][] ring : linearRings) {
                for (double[] point : ring) {
                    bytes += RamUsageEstimator.sizeOf(point);
                }
            }
            return bytes;
        }

        @Override
        public int compareTo(GeoShape o) {
            return 0;
        }
    }

    public static record MultiPolygon(double[][][][] polygons) implements GeoShape {
        @Override
        public long ramBytesUsed() {
            long bytes = 0;
            for (double[][][] polygon : polygons) {
                for (double[][] ring : polygon) {
                    for (double[] point : ring) {
                        bytes += RamUsageEstimator.sizeOf(point);
                    }
                }
            }
            return bytes;
        }

        @Override
        public int compareTo(GeoShape o) {
            return 0;
        }
    }

    public static record GeometryCollection(List<GeoShape> shapes) implements GeoShape {

        @Override
        public long ramBytesUsed() {
            return shapes.stream().mapToLong(GeoShape::ramBytesUsed).sum();
        }

        @Override
        public int compareTo(GeoShape o) {
            return 0;
        }
    }
}
