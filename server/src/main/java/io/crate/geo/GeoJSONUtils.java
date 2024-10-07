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
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Consumer;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.geo.parsers.ShapeParser;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.locationtech.jts.geom.Point;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.exception.InvalidShapeException;
import org.locationtech.spatial4j.shape.Circle;
import org.locationtech.spatial4j.shape.Rectangle;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.ShapeCollection;
import org.locationtech.spatial4j.shape.jts.JtsGeometry;
import org.locationtech.spatial4j.shape.jts.JtsPoint;

import io.crate.types.GeoPointType;
import io.crate.types.geo.GeoShape;

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

    public static GeoShape spatialShapeToGeoShape(Shape shape) {
        return switch (shape) {
            case JtsGeometry jtsGeometry -> null;
            case JtsPoint point -> new GeoShape.Point(new double[] { point.getX(), point.getY() });
            case Point point -> new GeoShape.Point(new double[] { point.getX(), point.getY() });
            case Rectangle rectangle -> null;
            case Circle circle -> null;
            case ShapeCollection<?> shapeCollection -> {
                List<GeoShape> shapes = new ArrayList<>(shapeCollection.size());
                for (Shape collShape : shapeCollection) {
                    shapes.add(spatialShapeToGeoShape(collShape));
                }
                yield new GeoShape.GeometryCollection(shapes);
            }
            default -> throw new IllegalArgumentException(String.format(
                Locale.ENGLISH,
                "Cannot convert shape %s to GeoShape",
                shape
            ));
        };
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
    public static GeoShape wkt2Map(String wkt) {
        return spatialShapeToGeoShape(wkt2Shape(wkt));
    }

    public static Shape map2Shape(Map<String, Object> geoJSONMap) {
        return geoJSONString2ShapeBuilder(geoJSONMap).buildS4J();
    }

    public static Object map2LuceneShape(Map<String, Object> geoJSONMap) {
        return geoJSONString2ShapeBuilder(geoJSONMap).buildLucene();
    }

    /*
     * TODO: avoid parsing to XContent and back to shape
     */
    private static ShapeBuilder geoJSONString2ShapeBuilder(Map<String, Object> geoJSONMap) {
        String geoJSON;
        try {
            geoJSON = Strings.toString(JsonXContent.builder().map(geoJSONMap));;
        } catch (Throwable e) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                "Cannot convert Map \"%s\" to shape", geoJSONMap), e);
        }
        try {
            XContentParser parser = JsonXContent.JSON_XCONTENT.createParser(
                NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, geoJSON);
            parser.nextToken();
            return ShapeParser.parse(parser);
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
}
