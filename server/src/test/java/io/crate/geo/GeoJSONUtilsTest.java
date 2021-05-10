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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Map;


import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.impl.CoordinateArraySequenceFactory;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.io.WKTWriter;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.jts.JtsGeometry;
import org.locationtech.spatial4j.shape.jts.JtsPoint;

public class GeoJSONUtilsTest {

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory(CoordinateArraySequenceFactory.instance());

    public final static List<Shape> SHAPES = List.<Shape>of(
        new JtsGeometry(new Polygon(GEOMETRY_FACTORY.createLinearRing(new Coordinate[]{
            new Coordinate(0.0, 1.0),
            new Coordinate(100.0, 0.1),
            new Coordinate(20.0, 23.567),
            new Coordinate(0.0, 1.0)
        }), new LinearRing[0], GEOMETRY_FACTORY), JtsSpatialContext.GEO, true, true),
        new JtsGeometry(new MultiPolygon(
            new Polygon[]{
                new Polygon(GEOMETRY_FACTORY.createLinearRing(new Coordinate[]{
                    new Coordinate(0.0, 1.0),
                    new Coordinate(0.1, 1.1),
                    new Coordinate(1.1, 60.0),
                    new Coordinate(0.0, 1.0)
                }), new LinearRing[0], GEOMETRY_FACTORY),
                new Polygon(GEOMETRY_FACTORY.createLinearRing(new Coordinate[]{
                    new Coordinate(2.0, 1.0),
                    new Coordinate(2.1, 1.1),
                    new Coordinate(2.1, 70.0),
                    new Coordinate(2.0, 1.0)
                }), new LinearRing[0], GEOMETRY_FACTORY)
            },
            GEOMETRY_FACTORY
        ), JtsSpatialContext.GEO, true, true),
        new JtsGeometry(GEOMETRY_FACTORY.createMultiPointFromCoords(new Coordinate[]{
            new Coordinate(0.0, 0.0),
            new Coordinate(1.0, 1.0)
        }), JtsSpatialContext.GEO, true, true),
        new JtsGeometry(GEOMETRY_FACTORY.createMultiLineString(new LineString[]{
            GEOMETRY_FACTORY.createLineString(new Coordinate[]{
                new Coordinate(0.0, 1.0),
                new Coordinate(0.1, 1.1),
                new Coordinate(1.1, 80.0),
                new Coordinate(0.0, 1.0)
            }),
            GEOMETRY_FACTORY.createLineString(new Coordinate[]{
                new Coordinate(2.0, 1.0),
                new Coordinate(2.1, 1.1),
                new Coordinate(2.1, 60.0),
                new Coordinate(2.0, 1.0)
            })
        }), JtsSpatialContext.GEO, true, true)

    );

    @Test
    public void testShape2Map() throws Exception {
        for (Shape shape : SHAPES) {
            Map<String, Object> map = GeoJSONUtils.shape2Map(shape);
            assertThat(map, hasKey("type"));
            GeoJSONUtils.validateGeoJson(map);
        }
    }

    @Test
    public void testPoint2Map() throws Exception {
        Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(0.0, 0.0));
        Shape shape = new JtsPoint(point, JtsSpatialContext.GEO);
        Map<String, Object> map = GeoJSONUtils.shape2Map(shape);
        assertThat(map, hasEntry("type", (Object) "Point"));
        assertThat(map.get("coordinates").getClass().isArray(), is(true));
        assertThat(((double[]) map.get("coordinates")).length, is(2));
    }

    @Test
    public void testMapFromWktRoundTrip() throws Exception {
        String wkt = "MULTILINESTRING ((10.05 10.28, 20.95 20.89), (20.95 20.89, 31.92 21.45))";
        Shape shape = GeoJSONUtils.wkt2Shape(wkt);
        Map<String, Object> map = GeoJSONUtils.shape2Map(shape);

        Map<String, Object> wktMap = GeoJSONUtils.wkt2Map(wkt);
        assertThat(map.get("type"), is(wktMap.get("type")));
        assertThat(map.get("coordinates"), is(wktMap.get("coordinates")));

        Shape mappedShape = GeoJSONUtils.map2Shape(map);
        String wktFromMap = new WKTWriter().toString(mappedShape);
        assertThat(wktFromMap, is(wkt));
    }

    @Test
    public void testInvalidWKT() throws Exception {
        assertThrows(IllegalArgumentException.class,
                     () -> GeoJSONUtils.wkt2Map(
                         "multilinestring (((10.05  10.28  3.4  8.4, 20.95  20.89  4.5  9.5),\n" +
                         " \n" +
                         "( 20.95  20.89  4.5  9.5, 31.92  21.45  3.6  8.6)))"),
                     "Cannot convert WKT \"multilinestring (((10.05  10.28  3.4  8.4, 20.95  20.89  4.5  9.5)");
    }

    @Test
    public void testMap2Shape() throws Exception {
        Shape shape = GeoJSONUtils.map2Shape(Map.<String, Object>of(
            GeoJSONUtils.TYPE_FIELD, GeoJSONUtils.LINE_STRING,
            GeoJSONUtils.COORDINATES_FIELD, new Double[][]{{0.0, 0.1}, {1.0, 1.1}}
        ));
        assertThat(shape, instanceOf(JtsGeometry.class));
        assertThat(((JtsGeometry) shape).getGeom(), instanceOf(LineString.class));

    }

    @Test
    public void testInvalidMap() throws Exception {
        assertThrows(IllegalArgumentException.class,
                     () -> GeoJSONUtils.map2Shape(Map.<String, Object>of()),
                     "Cannot convert Map \"{}\" to shape");
    }

    @Test
    public void testValidateMissingType() throws Exception {
        assertThrows(IllegalArgumentException.class,
                     () -> GeoJSONUtils.validateGeoJson(Map.of()),
                     "Invalid GeoJSON: type field missing");
    }

    @Test
    public void testValidateWrongType() throws Exception {
        assertThrows(IllegalArgumentException.class,
                     () -> GeoJSONUtils.validateGeoJson(Map.of(GeoJSONUtils.TYPE_FIELD, "Foo")),
                     "Invalid GeoJSON: invalid type");
    }

    @Test
    public void testValidateMissingCoordinates() throws Exception {
        assertThrows(IllegalArgumentException.class,
                     () -> GeoJSONUtils.validateGeoJson(Map.of(GeoJSONUtils.TYPE_FIELD, GeoJSONUtils.LINE_STRING)),
                     "Invalid GeoJSON: coordinates field missing");
    }

    @Test
    public void testValidateGeometriesMissing() throws Exception {
        assertThrows(IllegalArgumentException.class,
                     () -> GeoJSONUtils.validateGeoJson(Map.of(GeoJSONUtils.TYPE_FIELD, GeoJSONUtils.GEOMETRY_COLLECTION)),
                     "Invalid GeoJSON: coordinates field missing");
    }

    @Test
    public void testInvalidGeometryCollection() throws Exception {
        assertThrows(IllegalArgumentException.class,
                     () -> GeoJSONUtils.validateGeoJson(
                         Map.of(
                             GeoJSONUtils.TYPE_FIELD, GeoJSONUtils.GEOMETRY_COLLECTION,
                             GeoJSONUtils.GEOMETRIES_FIELD, List.<Object>of("ABC")
                         )
                     ),
                     "Invalid GeoJSON: invalid GeometryCollection");
    }

    @Test
    public void testValidateInvalidCoordinates() throws Exception {
        assertThrows(IllegalArgumentException.class,
                     () -> GeoJSONUtils.validateGeoJson(
                         Map.of(
                             GeoJSONUtils.TYPE_FIELD, GeoJSONUtils.POINT,
                             GeoJSONUtils.COORDINATES_FIELD, "ABC"
                         )
                     ),
                     "Invalid GeoJSON: invalid GeometryCollection");
    }

    @Test
    public void testInvalidNestedCoordinates() throws Exception {
        assertThrows(IllegalArgumentException.class,
                     () -> GeoJSONUtils.validateGeoJson(
                         Map.of(
                             GeoJSONUtils.TYPE_FIELD, GeoJSONUtils.POINT,
                             GeoJSONUtils.COORDINATES_FIELD, new double[][]{
                                 {0.0, 1.0},
                                 {1.0, 0.0}
                             }
                         )
                     ),
                     "Invalid GeoJSON: invalid coordinate");
    }

    @Test
    public void testInvalidDepthNestedCoordinates() throws Exception {
        assertThrows(IllegalArgumentException.class,
                     () -> GeoJSONUtils.validateGeoJson(
                         Map.of(
                             GeoJSONUtils.TYPE_FIELD, GeoJSONUtils.POLYGON,
                             GeoJSONUtils.COORDINATES_FIELD, new double[][]{
                                 {0.0, 1.0},
                                 {1.0, 0.0}
                             }
                         )
                     ),
                     "Invalid GeoJSON: invalid coordinate");
    }
}
