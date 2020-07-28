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

import io.crate.testing.TestingHelpers;
import io.crate.testing.UseRandomizedSchema;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static com.carrotsearch.randomizedtesting.RandomizedTest.$;
import static com.carrotsearch.randomizedtesting.RandomizedTest.$$;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.core.Is.is;

public class GeoShapeIntegrationTest extends SQLTransportIntegrationTest {

    private static final Map<String, Object> GEO_SHAPE1 = Map.of(
        "coordinates", new double[][]{
            {0, 0},
            {1, 1}
        },
        "type", "LineString"
    );
    private static final Map<String, Object> GEO_SHAPE2 = Map.of(
        "coordinates", new double[][]{
            {2, 2},
            {3, 3}
        },
        "type", "LineString"
    );

    @Before
    public void prepare() throws Exception {
        execute("CREATE TABLE shaped (" +
                "  id long primary key," +
                "  shape geo_shape," +
                "  shapes array(geo_shape)" +
                ") with (number_of_replicas=0)");
        ensureGreen();
        execute("INSERT INTO shaped (id, shape) VALUES (?, ?)", $$($(1L, "POINT (13.0 52.4)"), $(2L, GEO_SHAPE1)));
        execute("INSERT INTO shaped (id, shapes) VALUES (?, ?)", $(3, new Object[]{GEO_SHAPE1, GEO_SHAPE2}));
        execute("REFRESH TABLE shaped");
    }

    @Test
    public void testSelectGeoShapeFromSource() throws Exception {
        execute("select shape from shaped where id in (1, 2) order by id");
        assertThat(TestingHelpers.printedTable(response.rows()), is(
            "{coordinates=[13.0, 52.4], type=Point}\n" +
            "{coordinates=[[0.0, 0.0], [1.0, 1.0]], type=LineString}\n"));
//TODO: Re-enable once SQLResponse also includes the data types for the columns
//        assertThat(response.columnTypes()[0], is((DataType) DataTypes.GEO_SHAPE));
        assertThat(response.rows()[0][0], instanceOf(Map.class));
        assertThat(response.rows()[1][0], instanceOf(Map.class));

        execute("select shapes from shaped where id = 3");
        assertThat(TestingHelpers.printedTable(response.rows()), is(
            "[{coordinates=[[0.0, 0.0], [1.0, 1.0]], type=LineString}, " +
            "{coordinates=[[2.0, 2.0], [3.0, 3.0]], type=LineString}]\n"));
//TODO: Re-enable once SQLResponse also includes the data types for the columns
//        assertThat(response.columnTypes()[0], is((DataType) new ArrayType(DataTypes.GEO_SHAPE)));
        assertThat(response.rows()[0][0], instanceOf(List.class));
    }

    @Test
    @UseRandomizedSchema(random = false)
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
                          "      precision = '1.0m'\n" +
                          "   )\n" +
                          ")\n" +
                          "CLUSTERED INTO 1 SHARDS\n" +
                          "WITH (\n";
        assertEquals(response.rowCount(), 1L);
        assertThat((String) response.rows()[0][0], startsWith(expected));

        // execute the statement again and compare it with the SHOW CREATE TABLE result
        String stmt = (String) response.rows()[0][0];
        execute("drop table test");
        execute(stmt);
        ensureYellow();
        execute("show create table test");
        assertEquals(response.rows()[0][0], stmt);
    }

    @Test
    @UseRandomizedSchema(random = false)
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
        assertThat((String) response.rows()[0][0], startsWith(expected));
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
        assertThat(((long) response.rows()[0][0]), is(2L));
    }

    @Test
    public void testSelectWhereIntersects() throws Exception {
        execute("select id from shaped where intersects(shape, ?) order by id",
            $("POLYGON(" +
              "(12.995452 52.417497," +
              " 13.051071 52.424407," +
              " 13.053474 52.403047," +
              " 13.046951 52.400743," +
              " 13.069953 52.391944," +
              " 13.024635 52.354425," +
              " 12.970390 52.347714," +
              " 12.995452 52.417497))"));
        assertThat(response.rowCount(), is(1L));
        assertThat(response.rows()[0][0], is((Object) 1L));
    }

    @Test
    public void testGeoPointInPolygonQueryLuceneBug() {
        // Relates to https://github.com/elastic/elasticsearch/issues/20333
        // and fails if GeoPointInPolygonQuery is used in LuceneQueryBuilder.getPolygonQuery()
        execute("create table test(id integer, geopos geo_point)");
        ensureYellow();
        execute("insert into test (id, geopos) values(1, 'POINT(-0.35842 51.46961)')");
        execute("refresh table test");
        execute("select * from test where within(geopos, 'POLYGON((-0.129089 51.536726, -0.126686 51.536726, " +
                "-0.125999 51.536512, -0.125656 51.536512, -0.125312 51.536299, -0.125312 51.535444, " +
                "-0.125656 51.535231, -0.128402 51.53395, -0.128746 51.53395, -0.129432 51.533736, " +
                "-0.129776 51.533736, -0.130462 51.53395, -0.130805 51.53395, -0.131149 51.534163, " +
                "-0.131835 51.534804, -0.131835 51.535017, -0.131492 51.535444, -0.129776 51.536512, " +
                "-0.129089 51.536726))')");
        assertThat(response.rowCount(), is(0L));
    }
}
