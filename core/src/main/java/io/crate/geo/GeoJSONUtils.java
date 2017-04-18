/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.geo;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.vividsolutions.jts.geom.*;
import io.crate.core.collections.ForEach;
import io.crate.types.GeoPointType;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.exception.InvalidShapeException;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.ShapeCollection;

import java.lang.reflect.Array;
import java.util.*;

public class GeoJSONUtils {

    public static final String COORDINATES_FIELD = "coordinates";
    public static final String TYPE_FIELD = "type";
    static final String GEOMETRIES_FIELD = "geometries";

    // GEO JSON Types
    static final String GEOMETRY_COLLECTION = "GeometryCollection";
    public static final String POINT = "Point";
    private static final String MULTI_POINT = "MultiPoint";
    public static final String LINE_STRING = "LineString";
    private static final String MULTI_LINE_STRING = "MultiLineString";
    public static final String POLYGON = "Polygon";
    private static final String MULTI_POLYGON = "MultiPolygon";

    private static final ImmutableMap<String, String> GEOSJON_TYPES = ImmutableMap.<String, String>builder()
        .put(GEOMETRY_COLLECTION, GEOMETRY_COLLECTION)
        .put(GEOMETRY_COLLECTION.toLowerCase(Locale.ENGLISH), GEOMETRY_COLLECTION)
        .put(POINT, POINT)
        .put(POINT.toLowerCase(Locale.ENGLISH), POINT)
        .put(MULTI_POINT, MULTI_POINT)
        .put(MULTI_POINT.toLowerCase(Locale.ENGLISH), MULTI_POINT)
        .put(LINE_STRING, LINE_STRING)
        .put(LINE_STRING.toLowerCase(Locale.ENGLISH), LINE_STRING)
        .put(MULTI_LINE_STRING, MULTI_LINE_STRING)
        .put(MULTI_LINE_STRING.toLowerCase(Locale.ENGLISH), MULTI_LINE_STRING)
        .put(POLYGON, POLYGON)
        .put(POLYGON.toLowerCase(Locale.ENGLISH), POLYGON)
        .put(MULTI_POLYGON, MULTI_POLYGON)
        .put(MULTI_POLYGON.toLowerCase(Locale.ENGLISH), MULTI_POLYGON)
        .build();

    private static final GeoJSONMapConverter GEOJSON_CONVERTER = new GeoJSONMapConverter();

    public static Map<String, Object> shape2Map(Shape shape) {
        if (shape instanceof ShapeCollection) {
            ShapeCollection<?> shapeCollection = (ShapeCollection<?>) shape;
            List<Map<String, Object>> geometries = new ArrayList<>(shapeCollection.size());
            for (Shape collShape : shapeCollection) {
                geometries.add(shape2Map(collShape));
            }
            return ImmutableMap.of(
                TYPE_FIELD, GEOMETRY_COLLECTION,
                GEOMETRIES_FIELD, geometries
            );
        } else {
            try {
                return GEOJSON_CONVERTER.convert(JtsSpatialContext.GEO.getGeometryFrom(shape));
            } catch (InvalidShapeException e) {
                throw new IllegalArgumentException(
                    String.format(Locale.ENGLISH, "Cannot convert shape %s to Map", shape), e);
            }
        }
    }

    public static Shape wkt2Shape(String wkt) {
        try {
            return GeoPointType.WKT_READER.parse(wkt);
        } catch (Throwable e) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                "Cannot convert WKT \"%s\" to shape", wkt), e);
        }
    }

    /*
     * TODO: improve by directly parsing WKT 2 map
     */
    public static Map<String, Object> wkt2Map(String wkt) {
        return shape2Map(wkt2Shape(wkt));
    }

    /*
     * TODO: avoid parsing to XContent and back to shape
     */
    public static Shape map2Shape(Map<String, Object> geoJSONMap) {
        try {
            return geoJSONString2Shape(XContentFactory.jsonBuilder().map(geoJSONMap).string());
        } catch (Throwable e) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                "Cannot convert Map \"%s\" to shape", geoJSONMap), e);
        }
    }

    private static Shape geoJSONString2Shape(String geoJSON) {
        try {
            // FIXME is EMPTY safe here? Changing the method signature to get a NamedXContentRegistry is messy
            // FIXME but might be required. Test it!
            XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, geoJSON);
            parser.nextToken();
            return ShapeBuilder.parse(parser).build();
        } catch (Throwable t) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                "Cannot convert GeoJSON \"%s\" to shape", geoJSON), t);
        }
    }

    public static void validateGeoJson(Map value) {
        String type = BytesRefs.toString(value.get(TYPE_FIELD));
        if (type == null) {
            throw new IllegalArgumentException(invalidGeoJSON("type field missing"));
        }

        type = GEOSJON_TYPES.get(type);
        if (type == null) {
            throw new IllegalArgumentException(invalidGeoJSON("invalid type"));
        }

        if (GEOMETRY_COLLECTION.equals(type)) {
            Object geometries = value.get(GeoJSONUtils.GEOMETRIES_FIELD);
            if (geometries == null) {
                throw new IllegalArgumentException(invalidGeoJSON("geometries field missing"));
            }

            ForEach.forEach(geometries, new ForEach.Acceptor() {
                @Override
                public void accept(Object input) {
                    if (!(input instanceof Map)) {
                        throw new IllegalArgumentException(invalidGeoJSON("invalid GeometryCollection"));
                    } else {
                        validateGeoJson((Map) input);
                    }
                }
            });
        } else {
            Object coordinates = value.get(COORDINATES_FIELD);
            if (coordinates == null) {
                throw new IllegalArgumentException(invalidGeoJSON("coordinates field missing"));
            }
            switch (type) {
                case POINT:
                    validateCoordinate(coordinates);
                    break;
                case MULTI_POINT:
                case LINE_STRING:
                    validateCoordinates(coordinates, 1);
                    break;
                case POLYGON:
                case MULTI_LINE_STRING:
                    validateCoordinates(coordinates, 2);
                    break;
                case MULTI_POLYGON:
                    validateCoordinates(coordinates, 3);
                    break;
                default:
                    // shouldn't happen
                    throw new IllegalArgumentException(invalidGeoJSON("invalid type"));
            }
        }
    }

    private static void validateCoordinates(Object coordinates, final int depth) {
        ForEach.forEach(coordinates, new ForEach.Acceptor() {
            @Override
            public void accept(Object input) {
                if (depth > 1) {
                    validateCoordinates(input, depth - 1);
                } else {
                    // at coordinate level
                    validateCoordinate(input);
                }
            }
        });
    }

    private static void validateCoordinate(Object coordinate) {
        try {
            double x;
            double y;
            if (coordinate.getClass().isArray()) {
                Preconditions.checkArgument(Array.getLength(coordinate) == 2, invalidGeoJSON("invalid coordinate"));
                x = ((Number) Array.get(coordinate, 0)).doubleValue();
                y = ((Number) Array.get(coordinate, 1)).doubleValue();
            } else if (coordinate instanceof Collection) {
                Preconditions.checkArgument(
                    ((Collection) coordinate).size() == 2, invalidGeoJSON("invalid coordinate"));
                Iterator iter = ((Collection) coordinate).iterator();
                x = ((Number) iter.next()).doubleValue();
                y = ((Number) iter.next()).doubleValue();
            } else {
                throw new IllegalArgumentException(invalidGeoJSON("invalid coordinate"));
            }
            JtsSpatialContext.GEO.verifyX(x);
            JtsSpatialContext.GEO.verifyY(y);
        } catch (InvalidShapeException | ClassCastException e) {
            throw new IllegalArgumentException(invalidGeoJSON("invalid coordinate"), e);
        }
    }

    private static String invalidGeoJSON(String message) {
        return String.format(Locale.ENGLISH, "Invalid GeoJSON: %s", message);
    }

    /**
     * converts JTS geometries to geoJSON compatible Maps
     */
    private static class GeoJSONMapConverter {

        public Map<String, Object> convert(Geometry geometry) {
            ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();

            if (geometry instanceof Point) {
                builder.put(TYPE_FIELD, POINT)
                    .put(COORDINATES_FIELD, extract((Point) geometry));
            } else if (geometry instanceof MultiPoint) {
                builder.put(TYPE_FIELD, MULTI_POINT)
                    .put(COORDINATES_FIELD, extract((MultiPoint) geometry));
            } else if (geometry instanceof LineString) {
                builder.put(TYPE_FIELD, LINE_STRING)
                    .put(COORDINATES_FIELD, extract((LineString) geometry));
            } else if (geometry instanceof MultiLineString) {
                builder.put(TYPE_FIELD, MULTI_LINE_STRING)
                    .put(COORDINATES_FIELD, extract((MultiLineString) geometry));
            } else if (geometry instanceof Polygon) {
                builder.put(TYPE_FIELD, POLYGON)
                    .put(COORDINATES_FIELD, extract((Polygon) geometry));
            } else if (geometry instanceof MultiPolygon) {
                builder.put(TYPE_FIELD, MULTI_POLYGON)
                    .put(COORDINATES_FIELD, extract((MultiPolygon) geometry));
            } else if (geometry instanceof GeometryCollection) {
                GeometryCollection geometryCollection = (GeometryCollection) geometry;
                int size = geometryCollection.getNumGeometries();
                List<Map<String, Object>> geometries = new ArrayList<>(size);
                for (int i = 0; i < size; i++) {
                    geometries.add(convert(geometryCollection.getGeometryN(i)));
                }
                builder.put(TYPE_FIELD, GEOMETRY_COLLECTION)
                    .put(GEOMETRIES_FIELD, geometries);
            } else {
                throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                    "Cannot extract coordinates from geometry %s", geometry.getGeometryType()));
            }
            return builder.build();
        }

        private double[] extract(Point point) {
            return toArray(point.getCoordinate());
        }

        private double[][] extract(MultiPoint multiPoint) {
            return toArray(multiPoint.getCoordinates());
        }

        private double[][] extract(LineString lineString) {
            return toArray(lineString.getCoordinates());
        }

        private double[][][] extract(MultiLineString multiLineString) {
            int size = multiLineString.getNumGeometries();
            double[][][] lineStrings = new double[size][][];
            for (int i = 0; i < size; i++) {
                lineStrings[i] = toArray(multiLineString.getGeometryN(i).getCoordinates());
            }
            return lineStrings;
        }

        private double[][][] extract(Polygon polygon) {
            int size = polygon.getNumInteriorRing() + 1;
            double[][][] rings = new double[size][][];
            rings[0] = toArray(polygon.getExteriorRing().getCoordinates());
            for (int i = 0; i < size - 1; i++) {
                rings[i + 1] = toArray(polygon.getInteriorRingN(i).getCoordinates());
            }
            return rings;
        }

        private double[][][][] extract(MultiPolygon multiPolygon) {
            int size = multiPolygon.getNumGeometries();
            double[][][][] polygons = new double[size][][][];
            for (int i = 0; i < size; i++) {
                polygons[i] = extract((Polygon) multiPolygon.getGeometryN(i));
            }
            return polygons;
        }

        double[] toArray(Coordinate coordinate) {
            return new double[]{coordinate.x, coordinate.y};
        }

        double[][] toArray(Coordinate[] coordinates) {
            double[][] array = new double[coordinates.length][];
            for (int i = 0; i < coordinates.length; i++) {
                array[i] = toArray(coordinates[i]);
            }
            return array;
        }

    }
}
