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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.locationtech.spatial4j.shape.Shape;
import io.crate.geo.GeoJSONUtils;
import io.crate.geo.GeoJSONUtilsTest;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.*;

public class GeoShapeTypeTest extends CrateUnitTest {

    private static final List<String> WKT = ImmutableList.of(
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

    private static Map<String, Object> parse(String json) {
        try {
            return JsonXContent.jsonXContent.createParser(json).mapOrdered();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static final List<Map<String, Object>> GEO_JSON = ImmutableList.of(
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

    private GeoShapeType type = GeoShapeType.INSTANCE;

    @Test
    public void testStreamer() throws Exception {
        for (Map<String, Object> geoJSON : GEO_JSON) {
            Map<String, Object> value = type.value(geoJSON);

            BytesStreamOutput out = new BytesStreamOutput();
            type.streamer().writeValueTo(out, value);
            StreamInput in = out.bytes().streamInput();
            Map<String, Object> streamedValue = type.readValueFrom(in);

            assertThat(streamedValue.size(), is(value.size()));
            assertThat(streamedValue.get("type"), is(value.get("type")));
            assertThat(streamedValue.get("coordinates"), is(value.get("coordinates")));
        }
    }

    @Test
    public void testCompareValueTo() throws Exception {
        Map<String, Object> val1 = type.value("POLYGON ( (0 0, 20 0, 20 20, 0 20, 0 0 ))");
        Map<String, Object> val2 = type.value("POINT (10 10)");

        assertThat(type.compareValueTo(val1, val2), is(1));
        assertThat(type.compareValueTo(val2, val1), is(-1));
        assertThat(type.compareValueTo(val2, val2), is(0));
    }

    @Test
    public void testInvalidStringValueCausesIllegalArgumentException() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Cannot convert \"foobar\" to geo_shape");
        type.value("foobar");
    }

    @Test
    public void testInvalidTypeCausesIllegalArgumentException() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Cannot convert \"200\" to geo_shape");
        type.value(200);
    }

    @Test
    public void testInvalidCoordinates() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(allOf(
            startsWith("Cannot convert \""),
            endsWith("\" to geo_shape"))
        );
        type.value(ImmutableMap.of(
            GeoJSONUtils.TYPE_FIELD, GeoJSONUtils.LINE_STRING,
            GeoJSONUtils.COORDINATES_FIELD, new double[][]{
                new double[]{170.0d, 99.0d},
                new double[]{180.5d, -180.5d}
            }
        ));

    }

    @Test
    public void testConvertFromValidWKT() throws Exception {
        for (String wkt : WKT) {
            Map<String, Object> geoShape = type.value(wkt);
            assertThat(geoShape, is(notNullValue()));
        }
    }

    @Test
    public void testConvertFromValidGeoJSON() throws Exception {
        for (Map<String, Object> geoJSON : GEO_JSON) {
            Map<String, Object> geoShape = type.value(geoJSON);
            assertThat(geoShape, is(notNullValue()));
        }
    }

    @Test
    public void testNullValue() throws Exception {
        assertThat(type.value(null), is(nullValue()));
    }

    @Test
    public void testValueShape() throws Exception {
        for (Shape shape : GeoJSONUtilsTest.SHAPES) {
            Map<String, Object> map = type.value(shape);
            GeoJSONUtils.validateGeoJson(map);
        }
    }
}

