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

import static com.carrotsearch.randomizedtesting.RandomizedTest.assumeFalse;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.junit.Test;
import org.locationtech.spatial4j.shape.Shape;

import io.crate.geo.GeoJSONUtils;
import io.crate.geo.GeoJSONUtilsTest;

public class GeoShapeTypeTest extends DataTypeTestCase<Map<String, Object>> {

    private final List<String> wkt = List.of(
        "multipolygon empty",
        "MULTIPOLYGON (" +
        "  ((40 40, 20 45, 45 30, 40 40)),\n" +
        "  ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35), (30 20, 20 15, 20 25, 30 20))" +
        ")",
        "polygon (( 10 10, 10 20, 20 20, 20 15, 10 10))",
        "linestring ( 10.05  10.28 , 20.95  20.89 )",
        "multilinestring (( 10.05  10.28 , 20.95  20.89 ),( 20.95  20.89, 31.92 21.45))",
        "point ( 10.05  10.28 )",
        "multipoint (10 10, 20 20)"
    );

    private final GeoShapeType type = GeoShapeType.INSTANCE;

    @Override
    protected DataDef<Map<String, Object>> getDataDef() {
        return DataDef.fromType(type);
    }

    private static Map<String, Object> parse(String json) {
        try {
            return JsonXContent.JSON_XCONTENT.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                json
            ).mapOrdered();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private final List<Map<String, Object>> geoJson = List.of(
        parse("{ \"type\": \"Point\", \"coordinates\": [100.0, 0.0] }"),
        parse("{ \"type\": \"LineString\",\n" +
              "    \"coordinates\": [ [100.0, 0.0], [101.0, 1.0] ]\n" +
              "    }"),
        parse("{ \"type\": \"Polygon\",\n" +
              "    \"coordinates\": [\n" +
              "      [ [100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0] ]\n" +
              "      ]\n" +
              "   }"),
        parse("{ \"type\": \"Polygon\",\n" +
              "    \"coordinates\": [\n" +
              "      [ [100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0] ],\n" +
              "      [ [100.2, 0.2], [100.8, 0.2], [100.8, 0.8], [100.2, 0.8], [100.2, 0.2] ]\n" +
              "      ]\n" +
              "   }"),
        parse("{ \"type\": \"MultiPoint\",\n" +
              "    \"coordinates\": [ [100.0, 0.0], [101.0, 1.0] ]\n" +
              "    }"),
        parse("{ \"type\": \"MultiLineString\",\n" +
              "    \"coordinates\": [\n" +
              "        [ [100.0, 0.0], [101.0, 1.0] ],\n" +
              "        [ [102.0, 2.0], [103.0, 3.0] ]\n" +
              "      ]\n" +
              "    }"),
        parse("{ \"type\": \"MultiPolygon\",\n" +
              "    \"coordinates\": [\n" +
              "      [[[102.0, 2.0], [103.0, 2.0], [103.0, 3.0], [102.0, 3.0], [102.0, 2.0]]],\n" +
              "      [[[100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0]],\n" +
              "       [[100.2, 0.2], [100.8, 0.2], [100.8, 0.8], [100.2, 0.8], [100.2, 0.2]]]\n" +
              "      ]\n" +
              "    }"),
        parse("{ \"type\": \"GeometryCollection\",\n" +
              "    \"geometries\": [\n" +
              "      { \"type\": \"Point\",\n" +
              "        \"coordinates\": [100.0, 0.0]\n" +
              "        },\n" +
              "      { \"type\": \"LineString\",\n" +
              "        \"coordinates\": [ [101.0, 0.0], [102.0, 1.0] ]\n" +
              "        }\n" +
              "    ]\n" +
              "  }")
    );


    @Test
    public void testInvalidStringValueCausesIllegalArgumentException() throws Exception {
        assertThatThrownBy(() -> type.implicitCast("foobar"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Cannot convert WKT \"foobar\" to shape");
    }

    @Test
    public void testInvalidTypeCausesIllegalArgumentException() throws Exception {
        assertThatThrownBy(() -> type.implicitCast(200))
            .isExactlyInstanceOf(ClassCastException.class)
            .hasMessage("Can't cast '200' to geo_shape");
    }

    @Test
    public void testInvalidCoordinates() throws Exception {
        assertThatThrownBy(() -> type.implicitCast(Map.of(
                GeoJSONUtils.TYPE_FIELD, GeoJSONUtils.LINE_STRING,
                GeoJSONUtils.COORDINATES_FIELD, new double[][]{
                    new double[]{170.0d, 99.0d},
                    new double[]{180.5d, -180.5d}
                }
            )))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Invalid GeoJSON: invalid coordinate");
    }

    @Test
    public void testConvertFromValidWKT() throws Exception {
        for (String wkt : wkt) {
            Map<String, Object> geoShape = type.implicitCast(wkt);
            assertThat(geoShape).isNotNull();
        }
    }

    @Test
    public void testConvertFromValidGeoJSON() throws Exception {
        for (Map<String, Object> geoJSON : geoJson) {
            Map<String, Object> geoShape = type.implicitCast(geoJSON);
            assertThat(geoShape).isNotNull();
        }
    }

    @Test
    public void test_cast_with_null_value() {
        assertThat(type.implicitCast(null)).isNull();
    }

    @Test
    public void test_sanitize_value_geo_shape_objects() {
        for (Shape shape : GeoJSONUtilsTest.SHAPES) {
            Map<String, Object> map = type.sanitizeValue(shape);
            GeoJSONUtils.sanitizeMap(map);
        }
    }

    @Override
    public void test_reference_resolver_docvalues_off() throws Exception {
        assumeFalse("GeoShapeType cannot disable column store", true);
    }

    @Override
    public void test_reference_resolver_index_and_docvalues_off() throws Exception {
        assumeFalse("GeoShapeType cannot disable column store", true);
    }

    @Override
    public void test_reference_resolver_index_off() throws Exception {
        assumeFalse("GeoShapeType cannot disable index", true);
    }

    @Test
    public void test_bytes_estimate() throws Exception {
        assertThat(type.valueBytes(geoJson.get(0))).isEqualTo(848);
        assertThat(type.valueBytes(geoJson.get(1))).isEqualTo(1416);
        assertThat(type.valueBytes(geoJson.get(2))).isEqualTo(3040);
        assertThat(type.valueBytes(geoJson.get(3))).isEqualTo(5744);
        assertThat(type.valueBytes(geoJson.get(4))).isEqualTo(1416);
        assertThat(type.valueBytes(geoJson.get(5))).isEqualTo(2544);
        assertThat(type.valueBytes(geoJson.get(6))).isEqualTo(8504);
        assertThat(type.valueBytes(geoJson.get(7))).isEqualTo(2624);

        UnaryOperator<Map<String, Object>> normalize = geoJson -> {
            return type.implicitCast(GeoJSONUtils.map2Shape(geoJson));
        };
        assertThat(type.valueBytes(normalize.apply(geoJson.get(0)))).isEqualTo(264);
        assertThat(type.valueBytes(normalize.apply(geoJson.get(1)))).isEqualTo(320);
        assertThat(type.valueBytes(normalize.apply(geoJson.get(2)))).isEqualTo(424);
        assertThat(type.valueBytes(normalize.apply(geoJson.get(3)))).isEqualTo(600);
        assertThat(type.valueBytes(normalize.apply(geoJson.get(4)))).isEqualTo(824);
        assertThat(type.valueBytes(normalize.apply(geoJson.get(5)))).isEqualTo(424);
        assertThat(type.valueBytes(normalize.apply(geoJson.get(6)))).isEqualTo(1320);
        assertThat(type.valueBytes(normalize.apply(geoJson.get(7)))).isEqualTo(880);
    }

    @Test
    public void test_check_equality_with_itself() throws Exception {
        for (Map<String, Object> json: geoJson) {
            assertThat(type.compare(json, json)).isEqualTo(0);
        }
    }

    @Test
    public void test_check_topological_equality() throws Exception {
        // Both shapes have the same point set.
        // They are not equal exactly but they equal topologically.
        Map<String, Object> shape1 = type.implicitCast("polygon (( 0 0, 1 0, 1 1, 0 1, 0 0))");
        Map<String, Object> shape2 = type.implicitCast("polygon (( 1 0, 1 1, 0 1, 0 0, 1 0))");
        assertThat(type.compare(shape1, shape2)).isEqualTo(0);
        assertThat(type.compare(shape2, shape1)).isEqualTo(0);
    }

    @Test
    public void test_geometry_collections_with_same_shapes_in_different_order_are_not_equal() throws Exception {
        Map<String, Object> collection1 = parse(
            """
            {
                "type": "GeometryCollection",
                "geometries": [
                    {
                        "type": "Point",
                        "coordinates": [100.0, 0.0]
                    },
                    {
                        "type": "LineString",
                        "coordinates": [[101.0, 0.0], [102.0, 1.0]]
                    }
                ]
            }
            """
        );
        Map<String, Object> collection2 = parse(
            """
            {
                "type": "GeometryCollection",
                "geometries": [
                    {
                        "type": "LineString",
                        "coordinates": [[101.0, 0.0], [102.0, 1.0]]
                    },
                    {
                        "type": "Point",
                        "coordinates": [100.0, 0.0]
                    }
                ]
            }
            """
        );
        assertThat(type.compare(collection1, collection2)).isEqualTo(1);
        assertThat(type.compare(collection2, collection1)).isEqualTo(1);
    }

    @Test
    public void test_check_equality_of_collections_of_different_sizes() throws Exception {
        Map<String, Object> collection1 = parse(
            """
            {
                "type": "GeometryCollection",
                "geometries": [
                    {
                        "type": "Point",
                        "coordinates": [100.0, 0.0]
                    },
                    {
                        "type": "LineString",
                        "coordinates": [[101.0, 0.0], [102.0, 1.0]]
                    }
                ]
            }
            """
        );
        Map<String, Object> collection2 = parse(
            """
            {
                "type": "GeometryCollection",
                "geometries": [
                    {
                        "type": "Point",
                        "coordinates": [100.0, 0.0]
                    },
                    {
                        "type": "LineString",
                        "coordinates": [[101.0, 0.0], [102.0, 1.0]]
                    },
                    {
                        "type": "LineString",
                        "coordinates": [[101.0, 0.0], [102.0, 1.0]]
                    }
                ]
            }
            """
        );
        assertThat(type.compare(collection1, collection2)).isEqualTo(1);
        assertThat(type.compare(collection2, collection1)).isEqualTo(1);
    }

    @Test
    public void test_check_equality_of_shapes_of_same_type_with_different_coordinates_sizes() throws Exception {
        Map<String, Object> shape1 = parse("""
            {
                "type": "LineString",
                "coordinates": [[101.0, 0.0], [102.0, 1.0]]
            }
            """);
        Map<String, Object> shape2 = parse("""
            {
                "type": "LineString",
                "coordinates": [[101.0, 0.0], [102.0, 1.0], [103.0, 1.0]]
            }
            """);
        assertThat(type.compare(shape1, shape2)).isEqualTo(1);
        assertThat(type.compare(shape2, shape1)).isEqualTo(1);
    }

    @Test
    public void test_geometry_collection_equal_to_corresponding_multi_part() throws Exception {
        Map<String, Object> collection = parse(
            """
            {
                "type": "GeometryCollection",
                "geometries": [
                    {
                        "type": "Point",
                        "coordinates": [1.0, 1.0]
                    },
                    {
                        "type": "Point",
                        "coordinates": [2.0, 2.0]
                    }
                ]
            }
            """
        );
        Map<String, Object> multiPoint = parse("""
            {
                "type": "MultiPoint",
                "coordinates": [[1.0, 1.0], [2.0, 2.0]]
            }
            """
        );
        assertThat(type.implicitCast(collection)).isEqualTo(type.implicitCast(multiPoint));
        assertThat(type.sanitizeValue(collection)).isEqualTo(type.sanitizeValue(multiPoint));
    }
}

