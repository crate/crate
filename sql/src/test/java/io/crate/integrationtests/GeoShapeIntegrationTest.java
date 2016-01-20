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

package io.crate.integrationtests;

import com.google.common.collect.ImmutableMap;
import io.crate.testing.TestingHelpers;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.core.Is.is;

public class GeoShapeIntegrationTest extends SQLTransportIntegrationTest {

    @Before
    public void prepare() throws Exception {
        execute("CREATE TABLE shaped (" +
                "  id long primary key," +
                "  shape geo_shape" +
                ") with (number_of_replicas=0)");
        ensureGreen();
        // TODO: FIX ME! $ and $$ not available anymore
        /*execute("INSERT INTO shaped (id, shape) VALUES (?, ?)", $$(
                $(1L, "POINT (13.0 52.4)"),
                $(42L, ImmutableMap.of(
                        "type", "LineString",
                        "coordinates", new double[][] {
                                {0, 0},
                                {1, 1}
                        }
                ))
        ));*/
        execute("REFRESH TABLE shaped");
    }

    @Test
    public void testSelectGeoShapeFromSource() throws Exception {
        execute("select shape from shaped order by id");
        assertThat(TestingHelpers.printedTable(response.rows()), is(
                "{coordinates=[13.0, 52.4], type=Point}\n" +
                "{coordinates=[[0.0, 0.0], [1.0, 1.0]], type=LineString}\n"));
        assertThat(response.columnTypes()[0], is((DataType) DataTypes.GEO_SHAPE));
        assertThat(response.rows()[0][0], instanceOf(Map.class));
    }

    @Test
    public void testIndexWithES() throws Exception {
        execute("create table test (shape geo_shape)");
        ensureYellow();
        client().prepareIndex("test", "default", "test").setSource(jsonBuilder().startObject()
                     .startObject("shape")
                     .field("type", "polygon")
                     .startArray("coordinates").startArray()
                     .startArray().value(-122.83).value(48.57).endArray()
                     .startArray().value(-122.77).value(48.56).endArray()
                     .startArray().value(-122.79).value(48.53).endArray()
                     .startArray().value(-122.83).value(48.57).endArray() // close the polygon
                     .endArray().endArray()
                     .endObject()
                     .endObject()).execute().actionGet();
        execute("refresh table test");
        execute("select shape from test");
        assertThat(response.rowCount(), is(1L));
        assertThat(TestingHelpers.printedTable(response.rows()), is(
                "{coordinates=[[[-122.83, 48.57], [-122.77, 48.56], [-122.79, 48.53], [-122.83, 48.57]]], type=polygon}\n"));
        assertThat(response.columnTypes()[0], is((DataType) DataTypes.GEO_SHAPE));
        assertThat(response.rows()[0][0], instanceOf(Map.class));

    }

    @Test
    public void testShowCreateTable() throws Exception {
        execute("create table test (" +
                "col1 geo_shape INDEX using QUADTREE with (precision='1m', distance_error_pct='0.25')" +
                ") " +
                "CLUSTERED INTO 1 SHARDS");
        ensureYellow();
        execute("show create table test");
        String expected = "CREATE TABLE IF NOT EXISTS \"doc\".\"test\" (\n" +
                          "   \"col1\" GEO_SHAPE INDEX USING QUADTREE WITH (\n" +
                          "      distance_error_pct = 0.25,\n" +
                          "      tree_levels = 26\n" +
                          "   )\n" +
                          ")\n" +
                          "CLUSTERED INTO 1 SHARDS\n" +
                          "WITH (\n";
        assertEquals(response.rowCount(), 1L);
        assertThat((String)response.rows()[0][0], startsWith(expected));

        // execute the statement again and compare it with the SHOW CREATE TABLE result
        String stmt = (String)response.rows()[0][0];
        execute("drop table test");
        execute(stmt);
        ensureYellow();
        execute("show create table test");
        assertEquals(response.rows()[0][0], stmt);
    }

    @Test
    public void testShowCreateTableWithDefaultValues() throws Exception {
        execute("create table test (" +
                "col1 geo_shape" +
                ") " +
                "CLUSTERED INTO 1 SHARDS");
        ensureYellow();
        execute("show create table test");
        String expected = "CREATE TABLE IF NOT EXISTS \"doc\".\"test\" (\n" +
                          "   \"col1\" GEO_SHAPE INDEX USING GEOHASH\n" +
                          ")\n" +
                          "CLUSTERED INTO 1 SHARDS\n" +
                          "WITH (\n";
        assertEquals(response.rowCount(), 1L);
        assertThat((String)response.rows()[0][0], startsWith(expected));
    }

    @Test
    public void testGeoMatchTest() throws Exception {
        execute("select id from shaped where match(shape, " +
                "'POLYGON((12.998725175857544 52.40087142225922," +
                "13.002265691757202 52.40087142225922," +
                "13.002265691757202 52.39927416492016," +
                "12.998725175857544 52.39927416492016," +
                "12.998725175857544 52.40087142225922))')");
        assertThat(response.rowCount(), is(1L));
        assertThat(((long) response.rows()[0][0]), is(1L));

        execute("delete from shaped where match(shape, " +
                "{" +
                "   type='Polygon', " +
                "   coordinates=[[[12.998725175857544, 52.40087142225922], " +
                "                 [13.002265691757202, 52.40087142225922], " +
                "                 [12.998725175857544, 52.39927416492016], " +
                "                 [12.998725175857544, 52.40087142225922]]]})");
        execute("refresh table shaped");

        execute("select count(*) from shaped");
        assertThat(((long) response.rows()[0][0]), is(1L));
    }

    @Test
    public void testSelectWhereIntersects() throws Exception {
        // TODO: FIX ME! $ not available anymore
        /*execute("select id from shaped where intersects(shape, ?) order by id",
                $("POLYGON(" +
                     "(12.995452 52.417497," +
                     " 13.051071 52.424407," +
                     " 13.053474 52.403047," +
                     " 13.046951 52.400743," +
                     " 13.069953 52.391944," +
                     " 13.024635 52.354425," +
                     " 12.970390 52.347714," +
                     " 12.995452 52.417497))"));*/
        assertThat(response.rowCount(), is(1L));
        assertThat(response.rows()[0][0], is((Object)1L));

    }
}
