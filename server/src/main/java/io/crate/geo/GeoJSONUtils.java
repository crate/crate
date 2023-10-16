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

package io.crate.geo;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Consumer;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.geo.parsers.ShapeParser;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.exception.InvalidShapeException;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.ShapeCollection;

import io.crate.types.GeoPointType;

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

    private static final Map<String, String> GEOJSON_TYPES = Map.<String, String>ofEntries(
        Map.entry(GEOMETRY_COLLECTION, GEOMETRY_COLLECTION),
        Map.entry(GEOMETRY_COLLECTION.toLowerCase(Locale.ENGLISH), GEOMETRY_COLLECTION),
        Map.entry(POINT, POINT),
        Map.entry(POINT.toLowerCase(Locale.ENGLISH), POINT),
        Map.entry(MULTI_POINT, MULTI_POINT),
        Map.entry(MULTI_POINT.toLowerCase(Locale.ENGLISH), MULTI_POINT),
        Map.entry(LINE_STRING, LINE_STRING),
        Map.entry(LINE_STRING.toLowerCase(Locale.ENGLISH), LINE_STRING),
        Map.entry(MULTI_LINE_STRING, MULTI_LINE_STRING),
        Map.entry(MULTI_LINE_STRING.toLowerCase(Locale.ENGLISH), MULTI_LINE_STRING),
        Map.entry(POLYGON, POLYGON),
        Map.entry(POLYGON.toLowerCase(Locale.ENGLISH), POLYGON),
        Map.entry(MULTI_POLYGON, MULTI_POLYGON),
        Map.entry(MULTI_POLYGON.toLowerCase(Locale.ENGLISH), MULTI_POLYGON)
    );

    private static final GeoJSONMapConverter GEOJSON_CONVERTER = new GeoJSONMapConverter();

    /**
     * Invoke a consumer for each elements of a collection or an array
     *
     * @param arrayOrCollection a collection, a primitive or non-primitive array
     * @param consumer          called for every element of <code>arrayOrCollection</code> as Object
     */
    private static void forEach(Object arrayOrCollection, Consumer<Object> consumer) {
        if (arrayOrCollection.getClass().isArray()) {
            int arrayLength = Array.getLength(arrayOrCollection);
            for (int i = 0; i < arrayLength; i++) {
                Object elem = Array.get(arrayOrCollection, i);
                consumer.accept(elem);
            }
        } else if (arrayOrCollection instanceof Collection<?> collection) {
            collection.forEach(consumer);
        } else {
            throw new AssertionError("argument is neither an array nor a collection");
        }
    }

    public static Map<String, Object> shape2Map(Shape shape) {
        if (shape instanceof ShapeCollection) {
            ShapeCollection<?> shapeCollection = (ShapeCollection<?>) shape;
            List<Map<String, Object>> geometries = new ArrayList<>(shapeCollection.size());
            for (Shape collShape : shapeCollection) {
                geometries.add(shape2Map(collShape));
            }
            return Map.of(
                TYPE_FIELD, GEOMETRY_COLLECTION,
                GEOMETRIES_FIELD, geometries
            );
        } else {
            try {
                return GEOJSON_CONVERTER.convert(JtsSpatialContext.GEO.getShapeFactory().getGeometryFrom(shape));
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
            return geoJSONString2Shape(Strings.toString(JsonXContent.builder().map(geoJSONMap)));
        } catch (Throwable e) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                "Cannot convert Map \"%s\" to shape", geoJSONMap), e);
        }
    }

    private static Shape geoJSONString2Shape(String geoJSON) {
        try {
            XContentParser parser = JsonXContent.JSON_XCONTENT.createParser(
                NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, geoJSON);
            parser.nextToken();
            return ShapeParser.parse(parser).build();
        } catch (Throwable t) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                "Cannot convert GeoJSON \"%s\" to shape", geoJSON), t);
        }
    }

    public static void validateGeoJson(Map<?, ?> value) {
        String type = BytesRefs.toString(value.get(TYPE_FIELD));
        if (type == null) {
            throw new IllegalArgumentException(invalidGeoJSON("type field missing"));
        }

        type = GEOJSON_TYPES.get(type);
        if (type == null) {
            throw new IllegalArgumentException(invalidGeoJSON("invalid type"));
        }

        if (GEOMETRY_COLLECTION.equals(type)) {
            Object geometries = value.get(GeoJSONUtils.GEOMETRIES_FIELD);
            if (geometries == null) {
                throw new IllegalArgumentException(invalidGeoJSON("geometries field missing"));
            }

            forEach(geometries, input -> {
                if (!(input instanceof Map<?, ?> map)) {
                    throw new IllegalArgumentException(invalidGeoJSON("invalid GeometryCollection"));
                } else {
                    validateGeoJson(map);
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
        forEach(coordinates, input -> {
            if (depth > 1) {
                validateCoordinates(input, depth - 1);
            } else {
                // at coordinate level
                validateCoordinate(input);
            }
        });
    }

    private static void validateCoordinate(Object coordinate) {
        try {
            double x;
            double y;
            if (coordinate.getClass().isArray()) {
                assert Array.getLength(coordinate) == 2 : invalidGeoJSON("invalid coordinate");
                x = ((Number) Array.get(coordinate, 0)).doubleValue();
                y = ((Number) Array.get(coordinate, 1)).doubleValue();
            } else if (coordinate instanceof Collection) {
                assert ((Collection<?>) coordinate).size() == 2 : invalidGeoJSON("invalid coordinate");
                Iterator<?> iter = ((Collection<?>) coordinate).iterator();
                x = ((Number) iter.next()).doubleValue();
                y = ((Number) iter.next()).doubleValue();
            } else {
                throw new IllegalArgumentException(invalidGeoJSON("invalid coordinate"));
            }
            JtsSpatialContext.GEO.getShapeFactory().verifyX(x);
            JtsSpatialContext.GEO.getShapeFactory().verifyY(y);
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
            HashMap<String, Object> builder = new HashMap<>();

            if (geometry instanceof Point) {
                builder.put(TYPE_FIELD, POINT);
                builder.put(COORDINATES_FIELD, extract((Point) geometry));
            } else if (geometry instanceof MultiPoint) {
                builder.put(TYPE_FIELD, MULTI_POINT);
                builder.put(COORDINATES_FIELD, extract((MultiPoint) geometry));
            } else if (geometry instanceof LineString) {
                builder.put(TYPE_FIELD, LINE_STRING);
                builder.put(COORDINATES_FIELD, extract((LineString) geometry));
            } else if (geometry instanceof MultiLineString) {
                builder.put(TYPE_FIELD, MULTI_LINE_STRING);
                builder.put(COORDINATES_FIELD, extract((MultiLineString) geometry));
            } else if (geometry instanceof Polygon) {
                builder.put(TYPE_FIELD, POLYGON);
                builder.put(COORDINATES_FIELD, extract((Polygon) geometry));
            } else if (geometry instanceof MultiPolygon) {
                builder.put(TYPE_FIELD, MULTI_POLYGON);
                builder.put(COORDINATES_FIELD, extract((MultiPolygon) geometry));
            } else if (geometry instanceof GeometryCollection) {
                GeometryCollection geometryCollection = (GeometryCollection) geometry;
                int size = geometryCollection.getNumGeometries();
                List<Map<String, Object>> geometries = new ArrayList<>(size);
                for (int i = 0; i < size; i++) {
                    geometries.add(convert(geometryCollection.getGeometryN(i)));
                }
                builder.put(TYPE_FIELD, GEOMETRY_COLLECTION);
                builder.put(GEOMETRIES_FIELD, geometries);
            } else {
                throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                    "Cannot extract coordinates from geometry %s", geometry.getGeometryType()));
            }
            return Collections.unmodifiableMap(builder);
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
