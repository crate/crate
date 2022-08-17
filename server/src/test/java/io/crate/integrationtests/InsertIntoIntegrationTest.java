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

package io.crate.integrationtests;

import static com.carrotsearch.randomizedtesting.RandomizedTest.$;
import static com.carrotsearch.randomizedtesting.RandomizedTest.$$;
import static com.carrotsearch.randomizedtesting.RandomizedTest.randomAsciiLettersOfLength;
import static io.crate.protocols.postgres.PGErrorStatus.INTERNAL_ERROR;
import static io.crate.protocols.postgres.PGErrorStatus.UNIQUE_VIOLATION;
import static io.crate.testing.Asserts.assertThrowsMatches;
import static io.crate.testing.SQLErrorMatcher.isSQLError;
import static io.crate.testing.TestingHelpers.printedTable;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.CONFLICT;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.test.IntegTestCase;
import org.hamcrest.Matchers;
import org.hamcrest.core.IsNull;
import org.junit.Test;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.shape.impl.PointImpl;

import com.carrotsearch.randomizedtesting.annotations.Repeat;

import io.crate.common.collections.MapBuilder;
import io.crate.exceptions.VersioningValidationException;
import io.crate.testing.SQLResponse;
import io.crate.testing.UseJdbc;

@IntegTestCase.ClusterScope(numDataNodes = 2)
public class InsertIntoIntegrationTest extends IntegTestCase {

    private Setup setup = new Setup(sqlExecutor);

    @Test
    public void testInsertWithColumnNames() throws Exception {
        execute("create table test (\"firstName\" text, \"lastName\" text)");
        execute("insert into test (\"firstName\", \"lastName\") values ('Youri', 'Zoon')");
        assertEquals(1, response.rowCount());
        refresh();

        execute("select * from test where \"firstName\" = 'Youri'");

        assertEquals(1, response.rowCount());
        assertEquals("Youri", response.rows()[0][0]);
        assertEquals("Zoon", response.rows()[0][1]);
    }

    @Test
    public void testInsertWithoutColumnNames() throws Exception {
        execute("create table test (\"firstName\" string, \"lastName\" string)");
        ensureYellow();
        execute("insert into test values('Youri', 'Zoon')");
        assertEquals(1, response.rowCount());
        refresh();

        execute("select * from test where \"firstName\" = 'Youri'");

        assertEquals(1, response.rowCount());
        assertEquals("Youri", response.rows()[0][0]);
        assertEquals("Zoon", response.rows()[0][1]);
    }

    @Test
    public void testInsertAllCoreDatatypes() throws Exception {
        execute("create table test (" +
                "   boolean boolean," +
                "   datetime timestamptz," +
                "   double double," +
                "   float real," +
                "   integer integer," +
                "   long bigint," +
                "   short smallint," +
                "   string text" +
                ")");

        execute("insert into test values(true, '2013-09-10T21:51:43', 1.79769313486231570e+308, 3.402, 2147483647, 9223372036854775807, 32767, 'Youri')");
        execute("insert into test values(?, ?, ?, ?, ?, ?, ?, ?)",
            new Object[]{true, "2013-09-10T21:51:43", 1.79769313486231570e+308, 3.402, 2147483647, 9223372036854775807L, 32767, "Youri"});
        assertEquals(1, response.rowCount());
        refresh();

        execute("select * from test");

        assertEquals(2, response.rowCount());
        assertEquals(true, response.rows()[0][0]);
        assertEquals(1378849903000L, response.rows()[0][1]);
        assertEquals(1.79769313486231570e+308, response.rows()[0][2]);
        assertEquals(3.402f, ((Number) response.rows()[0][3]).floatValue(), 0.002f);
        assertEquals(2147483647, response.rows()[0][4]);
        assertEquals(9223372036854775807L, response.rows()[0][5]);
        assertEquals((short) 32767, response.rows()[0][6]);
        assertEquals("Youri", response.rows()[0][7]);

        assertEquals(true, response.rows()[1][0]);
        assertEquals(1378849903000L, response.rows()[1][1]);
        assertEquals(1.79769313486231570e+308, response.rows()[1][2]);
        assertEquals(3.402f, ((Number) response.rows()[1][3]).floatValue(), 0.002f);
        assertEquals(2147483647, response.rows()[1][4]);
        assertEquals(9223372036854775807L, response.rows()[1][5]);
        assertEquals((short) 32767, response.rows()[1][6]);
        assertEquals("Youri", response.rows()[1][7]);
    }

    @Test
    @UseJdbc(0) // avoid casting errors: Timestamp instead of Long
    public void testInsertCoreTypesAsArray() {
        execute("create table test (" +
                "\"boolean\" array(boolean), " +
                "\"datetime\" array(timestamp with time zone), " +
                "\"double\" array(double), " +
                "\"float\" array(float), " +
                "\"integer\" array(integer), " +
                "\"long\" array(long), " +
                "\"short\" array(short), " +
                "\"string\" array(string) " +
                ") with (number_of_replicas=0)"
        );
        ensureYellow();

        execute("insert into test values(?, ?, ?, ?, ?, ?, ?, ?)",
            new Object[]{
                new Boolean[]{true, false},
                new String[]{"2013-09-10T21:51:43", "2013-11-10T21:51:43"},
                new Double[]{1.79769313486231570e+308, 1.69769313486231570e+308},
                new Float[]{3.402f, 3.403f, null},
                new Integer[]{2147483647, 234583},
                new Long[]{9223372036854775807L, 4L},
                new Short[]{32767, 2},
                new String[]{"Youri", "Juri"}
            }
        );
        refresh();

        execute("select * from test");
        assertEquals(true, ((List) response.rows()[0][0]).get(0));
        assertEquals(false, ((List) response.rows()[0][0]).get(1));

        assertThat(((List) response.rows()[0][1]).get(0), is(1378849903000L));
        assertThat(((List) response.rows()[0][1]).get(1), is(1384120303000L));

        assertThat(((List) response.rows()[0][2]).get(0), is(1.79769313486231570e+308));
        assertThat(((List) response.rows()[0][2]).get(1), is(1.69769313486231570e+308));


        assertEquals(3.402f, ((Number) ((List) response.rows()[0][3]).get(0)).floatValue(), 0.002f);
        assertEquals(3.403f, ((Number) ((List) response.rows()[0][3]).get(1)).floatValue(), 0.002f);
        assertThat(((List) response.rows()[0][3]).get(2), nullValue());

        assertThat(((List) response.rows()[0][4]).get(0), is(2147483647));
        assertThat(((List) response.rows()[0][4]).get(1), is(234583));

        assertThat(((List) response.rows()[0][5]).get(0), is(9223372036854775807L));
        assertThat(((List) response.rows()[0][5]).get(1), is(4L));

        assertThat(((List) response.rows()[0][6]).get(0), is((short) 32767));
        assertThat(((List) response.rows()[0][6]).get(1), is((short) 2));

        assertThat(((List) response.rows()[0][7]).get(0), is("Youri"));
        assertThat(((List) response.rows()[0][7]).get(1), is("Juri"));
    }

    @Test
    public void testInsertBadIPAddress() throws Exception {
        execute("create table t (i ip) with (number_of_replicas=0)");
        ensureYellow();
        assertThrowsMatches(() -> execute("insert into t (i) values ('192.168.1.2'), ('192.168.1.3'),('192.168.1.500')"),
                     isSQLError(is("Cannot cast `'192.168.1.500'` of type `text` to type `ip`"),
                                INTERNAL_ERROR,
                                BAD_REQUEST,
                                4000));
    }

    @Test
    public void testInsertMultipleRows() throws Exception {
        execute("create table test (age integer, name text)");

        execute("insert into test values(32, 'Youri'), (42, 'Ruben')");
        assertEquals(2, response.rowCount());
        refresh();

        execute("select * from test order by \"name\"");

        assertEquals(2, response.rowCount());
        assertArrayEquals(new Object[]{42, "Ruben"}, response.rows()[0]);
        assertArrayEquals(new Object[]{32, "Youri"}, response.rows()[1]);
    }

    @Test
    public void testInsertWithParams() throws Exception {
        execute("create table test (age integer, name text)");

        Object[] args = new Object[]{32, "Youri"};
        execute("insert into test values(?, ?)", args);
        assertEquals(1, response.rowCount());
        refresh();

        execute("select * from test where name = 'Youri'");

        assertEquals(1, response.rowCount());
        assertEquals(32, response.rows()[0][0]);
        assertEquals("Youri", response.rows()[0][1]);
    }

    @Test
    public void testInsertWithParamsInScalar() throws Exception {
        execute("create table test (age integer, name string) with (number_of_replicas=0)");
        ensureYellow();
        Object[] args = new Object[]{"Youri"};
        execute("insert into test values(32, substr(?, 0, 2))", args);
        assertEquals(1, response.rowCount());
        refresh();

        execute("select * from test");
        assertEquals(1, response.rowCount());
        assertEquals(32, response.rows()[0][0]);
        assertEquals("Yo", response.rows()[0][1]);
    }

    @Test
    public void testInsertMultipleRowsWithParams() throws Exception {
        execute("create table test (age integer, name string) with (number_of_replicas=0)");
        ensureYellow();

        Object[] args = new Object[]{32, "Youri", 42, "Ruben"};
        execute("insert into test values(?, ?), (?, ?)", args);
        assertEquals(2, response.rowCount());
        refresh();

        execute("select * from test order by \"name\"");

        assertEquals(2, response.rowCount());
        assertArrayEquals(new Object[]{42, "Ruben"}, response.rows()[0]);
        assertArrayEquals(new Object[]{32, "Youri"}, response.rows()[1]);
    }

    @Test
    public void testInsertObject() throws Exception {
        execute("create table test (message string, person object) with (number_of_replicas=0)");
        ensureYellow();

        Map<String, Object> person = new HashMap<>();
        person.put("first_name", "Youri");
        person.put("last_name", "Zoon");
        Object[] args = new Object[]{"I'm addicted to kite", person};

        execute("insert into test values(?, ?)", args);
        assertEquals(1, response.rowCount());
        refresh();

        execute("select * from test");

        assertEquals(1, response.rowCount());
        assertArrayEquals(args, response.rows()[0]);
    }

    @Test
    @UseJdbc(0) // inserting object array equires special treatment for PostgreSQL
    public void testInsertEmptyObjectArray() throws Exception {
        execute("create table test (" +
                "  id integer primary key," +
                "  details array(object)" +
                ")");
        ensureYellow();
        execute("insert into test (id, details) values (?, ?)", new Object[]{1, new Map[0]});
        refresh();
        execute("select id, details from test");
        assertEquals(1, response.rowCount());
        assertEquals(1, response.rows()[0][0]);

        assertThat(response.rows()[0][1], instanceOf(List.class));
        List details = ((List) response.rows()[0][1]);
        assertThat(details.size(), is(0));
    }

    @Test
    public void testInsertWithPrimaryKey() throws Exception {
        this.setup.createTestTableWithPrimaryKey();

        Object[] args = new Object[]{"1",
            "A towel is about the most massively useful thing an interstellar hitch hiker can have."};
        execute("insert into test (pk_col, message) values (?, ?)", args);
        refresh();

        execute("select message from test where pk_col = '1'");
        assertThat((String) response.rows()[0][0], containsString("A towel is about the most"));
    }

    @Test
    public void testInsertWithPrimaryKeyFailing() throws Exception {
        this.setup.createTestTableWithPrimaryKey();

        execute("insert into test (pk_col, message) values (?, ?)", new Object[] {"1",
            "A towel is about the most massively useful thing an interstellar hitch hiker can have."});
        refresh();

        assertThrowsMatches(() -> execute("insert into test (pk_col, message) values (?, ?)",
                                          new Object[] {
                                              "1",
                                              "I always thought something was fundamentally wrong with the universe."
                                          }),
                            isSQLError(is("A document with the same primary key exists already"),
                                       UNIQUE_VIOLATION,
                                       CONFLICT,
                                       4091
                            )
        );
    }

    @Test
    public void testInsertWithPrimaryKeyMultiValues() throws Exception {
        this.setup.createTestTableWithPrimaryKey();

        Object[] args = new Object[]{
            "1", "All the doors in this spaceship have a cheerful and sunny disposition.",
            "2", "I always thought something was fundamentally wrong with the universe"
        };
        execute("insert into test (pk_col, message) values (?, ?), (?, ?)", args);
        refresh();

        execute("select message from test where pk_col = '1'");
        assertThat((String) response.rows()[0][0], containsString("All the doors"));
    }

    @Test
    public void testInsertWithPrimaryKeyMultiValuesFailing() throws Exception {
        execute("create table locations (id integer primary key)");
        ensureYellow();
        SQLResponse response = execute("insert into locations (id) values (1), (2)");
        assertThat(response.rowCount(), is(2L));
        response = execute("insert into locations (id) values (2), (3)");
        assertThat(response.rowCount(), is(1L));
    }

    @Test
    public void testInsertWithNotNullConstraintViolation() throws Exception {
        execute("create table t (pk_col int primary key, message string not null)");
        ensureYellow();

        Object[] args = new Object[]{"1", null};
        assertThrowsMatches(() -> execute("insert into t (pk_col, message) values (?, ?)", args),
                     isSQLError(is("\"message\" must not be null"), INTERNAL_ERROR, BAD_REQUEST, 4000));
    }

    @Test
    public void testInsertWithNotNull1LevelNestedColumn() {
        execute("create table test (" +
                "stuff object(dynamic) AS (" +
                "  level1 string not null" +
                ") not null)");
        ensureYellow();

        assertThrowsMatches(() -> execute("insert into test (stuff) values('{\"other_field\":\"value\"}')"),
                     isSQLError(is("\"stuff['level1']\" must not be null"), INTERNAL_ERROR, BAD_REQUEST, 4000));
    }

    @Test
    public void testInsertWithNotNull2LevelsNestedColumn() {
        execute("create table test (" +
                "stuff object(dynamic) AS (" +
                "  level1 object(dynamic) AS (" +
                "    level2 string not null" +
                "  ) not null" +
                ") not null)");
        ensureYellow();

        assertThrowsMatches(() -> execute("insert into test (stuff) values('{\"level1\":{\"other_field\":\"value\"}}')"),
                            isSQLError(is("\"stuff['level1']['level2']\" must not be null"),
                                       INTERNAL_ERROR,
                                       BAD_REQUEST,
                                       4000));
    }

    @Test
    public void testInsertWithUniqueConstraintViolation() throws Exception {
        this.setup.createTestTableWithPrimaryKey();

        Object[] args = new Object[] {
            "1", "All the doors in this spaceship have a cheerful and sunny disposition.",
        };
        execute("insert into test (pk_col, message) values (?, ?)", args);

        assertThrowsMatches(() -> execute("insert into test (pk_col, message) values (?, ?)",
                                          new Object[] {
                                              "1",
                                              "I always thought something was fundamentally wrong with the universe"}),
                            isSQLError(is("A document with the same primary key exists already"),
                                       UNIQUE_VIOLATION,
                                       CONFLICT,
                                       4091)
        );
    }

    @Test
    public void testInsertWithPKMissingOnInsert() throws Exception {
        this.setup.createTestTableWithPrimaryKey();

        Object[] args = new Object[]{
            "In the beginning the Universe was created.\n" +
                "This has made a lot of people very angry and been widely regarded as a bad move."
        };

        assertThrowsMatches(() -> execute("insert into test (message) values (?)", args),
                            isSQLError(is("Column `pk_col` is required but is missing from the insert statement"),
                                       INTERNAL_ERROR, BAD_REQUEST, 4000));
    }

    @Test
    public void testInsertWithClusteredByNull() throws Exception {
        execute("create table quotes (id integer, quote string) clustered by(id) " +
                "with (number_of_replicas=0)");
        ensureYellow();

        assertThrowsMatches(() -> execute("insert into quotes (id, quote) values(?, ?)",
                                   new Object[]{null, "I'd far rather be happy than right any day."}),
                     isSQLError(is("Clustered by value must not be NULL"), INTERNAL_ERROR, BAD_REQUEST, 4000));
    }

    @Test
    public void testInsertWithClusteredByWithoutValue() throws Exception {
        execute("create table quotes (id integer, quote string) clustered by(id) " +
                "with (number_of_replicas=0)");

        assertThrowsMatches(() -> execute("insert into quotes (quote) values(?)",
                                   new Object[]{"I'd far rather be happy than right any day."}),
                     isSQLError(is("Column `id` is required but is missing from the insert statement"),
                                INTERNAL_ERROR,
                                BAD_REQUEST, 4000));
    }


    @Test
    public void testInsertFromQueryWithSysColumn() throws Exception {
        execute("create table target (name string, a string, b string, docid int) " +
                "clustered into 1 shards with (number_of_replicas = 0)");
        execute("create table source (name string) clustered into 1 shards with (number_of_replicas = 0)");
        ensureYellow();

        execute("insert into source (name) values ('yalla')");
        execute("refresh table source");

        execute("insert into target (name, a, b, docid) (select name, _raw, _id, _docid from source)");
        execute("refresh table target");

        execute("select name, a, b, docid from target");
        assertThat(response.rows()[0][0], is("yalla"));
        assertThat(response.rows()[0][1], is("{\"name\":\"yalla\"}"));
        assertThat(response.rows()[0][2], IsNull.notNullValue());
        assertThat(response.rows()[0][3], is(0));
    }

    @Test
    public void testInsertFromQueryWithPartitionedTable() throws Exception {
        execute("create table users (id int primary key, name string) clustered into 2 shards " +
                "with (number_of_replicas = 0)");
        execute("create table users_parted (id int, name string) clustered into 1 shards " +
                "partitioned by (name) with (number_of_replicas = 0)");
        ensureYellow();


        execute("insert into users_parted (id, name) values (?, ?)", new Object[][]{
            new Object[]{1, "Arthur"},
            new Object[]{2, "Trillian"},
            new Object[]{3, "Marvin"},
            new Object[]{4, "Arthur"},
        });
        execute("refresh table users_parted");
        ensureYellow();

        execute("insert into users (id, name) (select id, name from users_parted)");
        execute("refresh table users");

        execute("select name from users order by name asc");
        assertThat(response.rowCount(), is(4L));
        assertThat(((String) response.rows()[0][0]), is("Arthur"));
        assertThat(((String) response.rows()[1][0]), is("Arthur"));
        assertThat(((String) response.rows()[2][0]), is("Marvin"));
        assertThat(((String) response.rows()[3][0]), is("Trillian"));
    }

    @Test
    public void test_insert_from_values_with_some_missing_partitioned_columns() {
        execute("CREATE TABLE PARTED (id INT, name TEXT, date TIMESTAMPTZ)" +
                "PARTITIONED BY (name, date)");

        execute("INSERT INTO parted (id, name) VALUES (?, ?)", new Object[]{1, "Trillian"});
        assertThat(response.rowCount(), is(1L));
        refresh();

        execute("SELECT table_name, partition_ident, values " +
                "FROM information_schema.table_partitions");
        assertThat(printedTable(response.rows()),
                   is("parted| 084l8sj9dhm6iobe00| {date=NULL, name=Trillian}\n"));
    }

    @Test
    public void test_insert_from_subquery_with_some_missing_partitioned_columns() {
        execute("CREATE TABLE parted (id INT, name TEXT, date TIMESTAMPTZ)" +
                "partitioned by (name, date)");

        execute("INSERT INTO parted (id, name) (SELECT ?, ?)", new Object[]{1, "Trillian"});
        assertThat(response.rowCount(), is(1L));
        refresh();

        execute("SELECT table_name, partition_ident, values " +
                "FROM information_schema.table_partitions");
        assertThat(printedTable(response.rows()),
                   is("parted| 084l8sj9dhm6iobe00| {date=NULL, name=Trillian}\n"));
    }

    @Test
    public void testInsertArrayLiteralFirstNull() throws Exception {
        execute("create table users(id int primary key, friends array(string), name string)");
        ensureYellow();
        execute("insert into users (id, friends, name) values (0, [null, 'gedöns'], 'björk')");
        execute("insert into users (id, friends, name) values (1, [null], null)");
        execute("refresh table users");
        execute("select friends, name from users order by id");
        assertThat(response.rowCount(), is(2L));

        List<Object> friends = (List<Object>) response.rows()[0][0];
        assertThat(friends.get(0), nullValue());
        assertThat(friends.get(1), is("gedöns"));
        assertThat(response.rows()[0][1], is("björk"));

        friends = ((List<Object>) response.rows()[1][0]);
        assertNull(friends.get(0));
    }

    @Test
    public void testInsertFromQueryGlobalAggregate() throws Exception {
        this.setup.setUpLocations();
        execute("refresh table locations");

        execute("create table aggs (" +
                " c long," +
                " s double" +
                ") with (number_of_replicas=0)");
        ensureYellow();

        execute("insert into aggs (c, s) (select count(*), sum(position) from locations)");
        assertThat(response.rowCount(), is(1L));

        execute("refresh table aggs");
        execute("select c, s from aggs");
        assertThat(response.rowCount(), is(1L));
        assertThat(((Number) response.rows()[0][0]).longValue(), is(13L));
        assertThat((Double) response.rows()[0][1], is(38.0));
    }

    @Test
    public void testInsertFromQueryCount() throws Exception {
        this.setup.setUpLocations();
        execute("refresh table locations");

        execute("create table aggs (" +
                " c long" +
                ") with (number_of_replicas=0)");
        ensureYellow();

        execute("insert into aggs (c) (select count(*) from locations)");
        assertThat(response.rowCount(), is(1L));

        execute("refresh table aggs");
        execute("select c from aggs");
        assertThat(response.rowCount(), is(1L));
        assertThat(((Number) response.rows()[0][0]).longValue(), is(13L));
    }

    @Test
    public void testInsertFromQuery() throws Exception {
        this.setup.setUpLocations();
        execute("refresh table locations");

        execute("select * from locations order by id");
        Object[][] rowsOriginal = response.rows();

        execute("create table locations2 (" +
                " id string primary key," +
                " name string," +
                " date timestamp with time zone," +
                " kind string," +
                " position long," +         // <<-- original type is integer, testing implicit cast
                " description string," +
                " race object" +
                ") clustered by(id) into 2 shards with(number_of_replicas=0)");

        execute("insert into locations2 (select * from locations)");
        assertThat(response.rowCount(), is(13L));

        execute("refresh table locations2");
        execute("select * from locations2 order by id");
        assertThat(response.rowCount(), is(13L));

        for (int i = 0; i < rowsOriginal.length; i++) {
            rowsOriginal[i][4] = (long) ((int) rowsOriginal[i][4]);
        }
        assertThat(response.rows(), is(rowsOriginal));
    }

    @Test
    public void testInsertFromQueryWithGeoType() throws Exception {
        execute("create table t (p geo_point) clustered into 1 shards with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into t (p) values (?)", new Object[]{new Double[]{10.d, 10.d}});
        execute("refresh table t");
        execute("create table t2 (p geo_point) clustered into 1 shards with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into t2 (p) (select p from t)");
        assertThat(response.rowCount(), is(1L));
    }

    @Test
    @UseJdbc(0) // inserting geo-point array requires special treatment for PostgreSQL
    public void testInsertIntoGeoPointArray() throws Exception {
        execute("create table t (id int, points array(geo_point)) clustered into 1 shards with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into t (id, points) values (1, [[1.1, 2.2],[3.3, 4.4]])");
        execute("insert into t (id, points) values (2, ['POINT(5.5 6.6)','POINT(7.7 8.8)'])");
        execute("insert into t (id, points) values (?, ?)",
            new Object[]{3, new Double[][]{new Double[]{9.9, 10.10}, new Double[]{11.11, 12.12}}});
        execute("refresh table t");
        execute("select points from t order by id");
        assertThat(response.rowCount(), is(3L));
        assertThat(
            (List<Object>) response.rows()[0][0],
            contains(
                is(new PointImpl(1.1, 2.2, JtsSpatialContext.GEO)),
                is(new PointImpl(3.3, 4.4, JtsSpatialContext.GEO))
            )
        );
        assertThat(
            (List<Object>) response.rows()[1][0],
            contains(
                is(new PointImpl(5.5, 6.6, JtsSpatialContext.GEO)),
                is(new PointImpl(7.7, 8.8, JtsSpatialContext.GEO))
            )
        );
        assertThat(
            (List<Object>) response.rows()[2][0],
            contains(
                is(new PointImpl(9.9, 10.10, JtsSpatialContext.GEO)),
                is(new PointImpl(11.11, 12.12, JtsSpatialContext.GEO))
            )
        );
    }

    @Test
    public void testInsertFromQueryWithAggregateWithinScalarFunction() throws Exception {
        this.setup.setUpCharacters();
        waitNoPendingTasksOnAll();
        execute("create table t (count int, id int) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into t (count, id) (select (count(*) + 1), id from characters group by id)");
        refresh();
        execute("select count, id from t order by id");
        assertThat(response.rowCount(), is(4L));
        assertThat((int) response.rows()[0][0], is(2));
        assertThat((int) response.rows()[1][0], is(2));
        assertThat((int) response.rows()[2][0], is(2));
        assertThat((int) response.rows()[3][0], is(2));
        assertThat((int) response.rows()[0][1], is(1));
        assertThat((int) response.rows()[1][1], is(2));
        assertThat((int) response.rows()[2][1], is(3));
        assertThat((int) response.rows()[3][1], is(4));
    }

    @Test
    public void testInsertFromSubQueryNonDistributedGroupBy() throws Exception {
        // resolve number of nodes, used for validating that all rows were inserted
        execute("select count(*) from sys.nodes");
        long numNodes = (long) response.rows()[0][0];

        execute("create table nodes (count integer, name string) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into nodes (count, name) (select count(*), name from sys.nodes group by name)");
        refresh();
        execute("select * from nodes");
        assertThat(response.rowCount(), is(numNodes));
    }

    @Test
    public void testInsertFromSubQueryDistributedGroupBy() throws Exception {
        this.setup.setUpCharacters();
        execute("create table t (id int, name string)");
        ensureYellow();
        execute("insert into t (id, name) (select id, name from characters group by id, name)");
        assertThat(response.rowCount(), is(4L));
        refresh();
        execute("select id, name from t order by id");
        assertThat(response.rowCount(), is(4L));
        assertThat((int) response.rows()[3][0], is(4));
        assertThat((String) response.rows()[3][1], is("Arthur"));

    }

    @Test
    public void testInsertFromValuesOnDuplicateKey() {
        execute("create table t1 (id integer primary key, other string) clustered into 1 shards");
        execute("insert into t1 (id, other) values (1, 'test'), (2, 'test2')");

        execute("insert into t1 (id, other) values (1, 'updated') ON CONFLICT (id) DO UPDATE SET other = 'updated'");
        assertThat(response.rowCount(), is(1L));
        refresh();

        execute("select id, other from t1 order by id");
        assertThat(printedTable(response.rows()), is(
            "1| updated\n" +
            "2| test2\n")
        );

        execute("insert into t1 (id, other) values (1, 'updated_again'), (3, 'new') ON CONFLICT (id) DO UPDATE SET other = excluded.other");
        assertThat(response.rowCount(), is(2L));
        refresh();

        execute("select id, other from t1 order by id");
        assertThat(printedTable(response.rows()), is(
            "1| updated_again\n" +
            "2| test2\n" +
            "3| new\n")
        );
    }

    @Test
    public void testInsertFromQueryOnDuplicateKey() throws Exception {
        setup.setUpCharacters();
        waitNoPendingTasksOnAll();
        execute("create table t (id integer primary key, name string, female boolean)");

        // copy all over from 'characters' table
        execute("insert into t (id, name, female) (select id, name, female from characters)");
        assertThat(response.rowCount(), is(4L));
        refresh();

        execute("select female, count(*) from t group by female order by female");
        assertThat(response.rowCount(), is(2L));
        assertThat((Long) response.rows()[0][1], is(2L));

        // set all 'female' values to true
        execute("insert into t (id, name, female) (select id, name, female from characters) " +
                "on conflict (id) do update set female = ?",
            new Object[]{true});
        assertThat(response.rowCount(), is(4L));
        refresh();

        execute("select female, count(*) from t group by female");
        assertThat(response.rowCount(), is(1L));
        assertThat((Long) response.rows()[0][1], is(4L));

        // set all 'female' values back to their original values
        execute("insert into t (id, name, female) (select id, name, female from characters) " +
                "on conflict (id) do update set female = excluded.female");
        assertThat(response.rowCount(), is(4L));
        refresh();

        execute("select female, count(*) from t group by female order by female");
        assertThat(response.rowCount(), is(2L));
        assertThat(printedTable(response.rows()),
            is("false| 2\n" +
               "true| 2\n"));
    }

    @Test
    public void testInsertFromValuesOnConflictDoNothing() {
        execute("create table t1 (id integer primary key, other string) clustered into 1 shards");
        execute("insert into t1 (id, other) values (1, 'test'), (2, 'test2')");

        execute("insert into t1 (id, other) values (1, 'updated') ON CONFLICT DO NOTHING");
        assertThat(response.rowCount(), is(0L));
        execute("insert into t1 (id, other) values (1, 'updated') ON CONFLICT (id) DO NOTHING");
        assertThat(response.rowCount(), is(0L));
        // the statement below also succeeds without ON CONFLICT DO NOTHING because we allow errors for multiple values
        execute("insert into t1 (id, other) values (1, 'updated'), (3, 'new') ON CONFLICT DO NOTHING");
        assertThat(response.rowCount(), is(1L));
        refresh();

        execute("select id, other from t1 order by id");
        assertThat(printedTable(response.rows()), is(
            "1| test\n" +
            "2| test2\n" +
            "3| new\n")
        );
    }

    @Test
    public void testInsertFromQueryOnConflictDoNothing() {
        execute("create table t1 (id integer primary key, other string) clustered into 1 shards");
        execute("insert into t1 (id, other) values (1, 'test'), (2, 'test2')");

        // these statements succeed even without ON CONFLICT DO NOTHING because we allow errors for subqueries
        execute("insert into t1 (id, other) (select * from unnest([1, 4], ['updated', 'another'])) ON CONFLICT DO NOTHING");
        assertThat(response.rowCount(), is(1L));
        execute("insert into t1 (id, other) (select * from unnest([1, 4], ['updated', 'another'])) ON CONFLICT (id) DO NOTHING");
        assertThat(response.rowCount(), is(0L));
        refresh();

        execute("select id, other from t1 order by id");
        assertThat(printedTable(response.rows()), is(
            "1| test\n" +
            "2| test2\n" +
            "4| another\n")
        );
    }

    @Test
    public void testInsertFromSubQueryWithVersion() throws Exception {
        execute("create table users (name string) clustered into 1 shards");

        assertThrowsMatches(() -> execute("insert into users (name) (select name from users where _version = 1)"),
                     isSQLError(containsString(VersioningValidationException.VERSION_COLUMN_USAGE_MSG),
                                INTERNAL_ERROR,
                                BAD_REQUEST,
                                4000));
    }

    @Test
    public void testInsertFromSubQueryPartitionedTableCustomSchema() throws Exception {
        execute("create table custom.source (" +
                "  name string, " +
                "  zipcode string, " +
                "  city string" +
                ") clustered into 5 shards " +
                "partitioned by (city) with (number_of_replicas=0)");
        execute("create table custom.destination (" +
                "  name string, " +
                "  zipcode string, " +
                "  city string" +
                ") clustered into 5 shards " +
                "partitioned by (zipcode) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into custom.source (name, zipcode, city) values (?, ?, ?)", new Object[][]{
            {"Schulz", "10243", "Berlin"},
            {"Dings", "14713", "Leipzig"},
            {"Foo", "10243", "Musterhausen"}
        });
        ensureYellow();
        refresh();

        execute("select table_name, table_schema, partition_ident, values, number_of_shards, number_of_replicas " +
                "from information_schema.table_partitions where table_schema='custom' and table_name='source' order by partition_ident");
        String[] rows = printedTable(response.rows()).split("\n");
        assertThat(rows[0], is("source| custom| 043k4pbidhkms| {city=Berlin}| 5| 0"));
        assertThat(rows[1], is("source| custom| 0444opb9e1t6ipo| {city=Leipzig}| 5| 0"));
        assertThat(rows[2], is("source| custom| 046kqtbjehin4q31elpmarg| {city=Musterhausen}| 5| 0"));

        execute("insert into custom.destination (select * from custom.source)");
        assertThat(response.rowCount(), is(3L));
        ensureYellow();
        refresh();

        execute("select city, name, zipcode from custom.destination order by city");
        assertThat(printedTable(response.rows()), is(
            "Berlin| Schulz| 10243\n" +
            "Leipzig| Dings| 14713\n" +
            "Musterhausen| Foo| 10243\n"));

        execute("select table_name, table_schema, partition_ident, values, number_of_shards, number_of_replicas " +
                "from information_schema.table_partitions where table_schema='custom' and table_name='destination' order by partition_ident");
        rows = printedTable(response.rows()).split("\n");
        assertThat(rows[0], is("destination| custom| 04332c1i6gpg| {zipcode=10243}| 5| 0"));
        assertThat(rows[1], is("destination| custom| 04332d1n64pg| {zipcode=14713}| 5| 0"));

    }

    @Test
    public void testInsertFromSubQueryGeoShapes() throws Exception {
        execute("create table strshapes (id int primary key, shape string) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into strshapes (id, shape) VALUES (?, ?)", $$(
            $(1, "POINT (0 0)"),
            $(2, "LINESTRING (0 0, 1 1, 2 2)")
        ));
        execute("refresh table strshapes");

        execute("create table shapes (id int primary key, shape geo_shape) with (number_of_replicas=0)");
        ensureYellow();

        execute("insert into shapes (id, shape) (select id, shape from strshapes)");
        execute("refresh table shapes");

        execute("select * from shapes order by id");
        assertThat(printedTable(response.rows()), is(
            "1| {coordinates=[0.0, 0.0], type=Point}\n" +
            "2| {coordinates=[[0.0, 0.0], [1.0, 1.0], [2.0, 2.0]], type=LineString}\n"));

    }

    @Test
    public void testBulkInsertWithNullValue() throws Exception {
        execute("create table t (x int)");

        Object[][] bulkArgs = new Object[][]{new Object[]{null}};
        long[] rowCounts = execute("insert into t values (?)", bulkArgs);
        assertThat(rowCounts, is(new long[] { 1L }));

        bulkArgs = new Object[][]{
            new Object[]{10},
            new Object[]{null},
            new Object[]{20}
        };
        rowCounts = execute("insert into t values (?)", bulkArgs);
        assertThat(rowCounts, is(new long[] { 1L, 1L, 1L }));

        refresh();
        execute("select * from t");
        assertThat(response.rowCount(), is(4L));
    }

    @Test
    public void testBulkInsertWithMultiValue() throws Exception {
        execute("create table t (x int)");
        Object[][] bulkArgs = {
            new Object[]{10, 11},
            new Object[]{20, 21},
            new Object[]{30, 31}
        };
        long[] rowCounts = execute("insert into t values (?), (?)", bulkArgs);
        assertThat(rowCounts, is(new long[] { 2L, 2L, 2L }));
    }

    @Test
    public void testBulkInsertWithMultiValueFailing() throws Exception {
        execute("create table t (x int primary key)");
        Object[][] bulkArgs = new Object[][]{
            new Object[]{10, 11},
            new Object[]{20, 21},
        };
        long[] rowCounts = execute("insert into t values (?), (?)", bulkArgs);
        assertThat(rowCounts, is(new long[] { 2L, 2L }));

        bulkArgs = new Object[][]{
            new Object[]{20, 21},
            new Object[]{30, 31},
        };
        rowCounts = execute("insert into t values (?), (?)", bulkArgs);
        assertThat(rowCounts, is(new long[] { -2L, 2L }));
    }

    @Test
    public void testBulkInsert() {
        execute("create table giveittome (" +
                "  date timestamp with time zone," +
                "  dirty_names array(string)," +
                "  lashes short primary key" +
                ") with (number_of_replicas=0)");
        ensureYellow();
        int bulkSize = randomIntBetween(1, 250);
        Object[][] bulkArgs = new Object[bulkSize][];
        for (int i = 0; i < bulkSize; i++) {
            bulkArgs[i] = new Object[]{System.currentTimeMillis() +
                                       i, new String[]{randomAsciiLettersOfLength(5), randomAsciiLettersOfLength(2)}, (short) i};
        }
        long[] rowCounts = execute("insert into giveittome (date, dirty_names, lashes) values (?, ?, ?)", bulkArgs);
        assertThat(rowCounts.length, is(bulkSize));
        execute("refresh table giveittome");
        // assert that bulk insert has inserted everything it said it has
        execute("select sum(lashes), date from giveittome group by date");
        assertThat(response.rowCount(), is((long) bulkSize));
    }

    @Test
    public void testBulkInsertWithFailing() throws Exception {
        execute("create table locations (id integer primary key, name string) with (number_of_replicas=0)");
        long[] rowCounts = execute("insert into locations (id, name) values (?, ?)", $$($(1, "Mars"), $(1, "Sun")));
        assertThat(rowCounts, is(new long[] { 1L, -2L }));
    }

    @Test
    public void testInsertIntoLongPartitionedBy() throws Exception {
        execute("create table import (col1 int, col2 long primary key) partitioned by (col2)");
        execute("insert into import (col1, col2) values (1, 1)");
        assertThat(response.rowCount(), is(1L));
        refresh();
        execute("select * from import");
        assertThat(printedTable(response.rows()), is("1| 1\n"));
    }

    @Test
    public void testInsertWithGeneratedColumn() {
        execute("create table test_generated_column (" +
                " ts timestamp with time zone," +
                " day as date_trunc('day', ts)," +
                " \"user\" object as (name string)," +
                " name as concat(\"user\"['name'], 'bar')" +
                ") with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into test_generated_column (ts, \"user\") values (?, ?)", new Object[][]{
            new Object[]{"2015-11-18T11:11:00", MapBuilder.newMapBuilder().put("name", "foo").map()},
            new Object[]{"2015-11-18T17:41:00", null},
        });
        refresh();
        execute("select ts, day, name from test_generated_column order by ts");
        assertThat(response.rows()[0][0], is(1447845060000L));
        assertThat(response.rows()[0][1], is(1447804800000L));
        assertThat(response.rows()[0][2], is("foobar"));

        assertThat(response.rows()[1][0], is(1447868460000L));
        assertThat(response.rows()[1][1], is(1447804800000L));
        assertThat(response.rows()[1][2], is("bar"));
    }

    @Test
    public void testInsertMutipleRowsWithGeneratedColumn() throws Exception {
        execute("CREATE TABLE computed (\n" +
                "   dividend double,\n" +
                "   divisor double,\n" +
                "   quotient AS (dividend / divisor)\n" +
                ")");
        ensureYellow();
        execute("INSERT INTO computed (dividend, divisor) VALUES (1.0, 1.0), (0.0, 10.0)");
        assertThat(response.rowCount(), is(2L));
        execute("refresh table computed");

        execute("select * from computed order by quotient");
        assertThat(printedTable(response.rows()), is(
            "0.0| 10.0| 0.0\n" +
            "1.0| 1.0| 1.0\n"));

    }

    @Test
    public void testInsertOnDuplicateWithGeneratedColumn() {
        execute("create table test_generated_column (" +
                " id integer primary key," +
                " ts timestamp with time zone," +
                " day as date_trunc('day', ts)" +
                ") with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into test_generated_column (id, ts) values (?, ?)", new Object[]{
            1, "2015-11-18T11:11:00Z"});
        refresh();

        execute("insert into test_generated_column (id, ts) values (?, ?)" +
                "on conflict (id) do update set ts = ?",
            new Object[]{1, "2015-11-18T11:11:00Z", "2015-11-23T14:43:00Z"});
        refresh();

        execute("select ts, day from test_generated_column");
        assertThat(response.rows()[0][0], is(1448289780000L));
        assertThat(response.rows()[0][1], is(1448236800000L));
    }

    @Test
    public void testInsertOnCurrentTimestampGeneratedColumn() {
        execute("create table t (" +
            "id int, " +
            "created timestamp with time zone generated always as current_timestamp)");
        ensureYellow();
        execute("insert into t (id) values(1)");
        execute("refresh table t");
        execute("select id, created from t");
        assertThat(response.rows()[0][0], is(1));
        assertThat(response.rows()[0][1], notNullValue());
    }

    @Test
    public void testInsertOnCurrentSchemaGeneratedColumn() {
        execute("create table t (id int, schema string generated always as current_schema)", (String) null);
        execute("create table t (id int, schema string generated always as current_schema)", "foo");
        ensureYellow();

        execute("insert into t (id) values (1)", (String) null);
        execute("refresh table t", (String) null);
        execute("select id, schema from t", (String) null);
        assertThat((Integer) response.rows()[0][0], is(1));
        assertThat((String) response.rows()[0][1], is("doc"));

        execute("insert into t (id) values (2)", "foo");
        execute("refresh table t", "foo");
        execute("select id, schema from t", "foo");
        assertThat((Integer) response.rows()[0][0], is(2));
        assertThat((String) response.rows()[0][1], is("foo"));
    }

    @Test
    public void testInsertNullSourceForNotNullGeneratedColumn() {
        execute("create table generated_column (" +
                " id int primary key," +
                " ts timestamp with time zone," +
                " gen_col as extract(year from ts) not null" +
                ") with (number_of_replicas=0)");
        ensureYellow();

        assertThrowsMatches(() -> execute("insert into generated_column (id, ts) values (1, null)"),
                     isSQLError(is("\"gen_col\" must not be null"), INTERNAL_ERROR, BAD_REQUEST, 4000));
    }

    @Test
    public void testInsertNullTargetForNotNullGeneratedColumn() {
        execute("create table generated_column (" +
                " id int primary key," +
                " ts timestamp with time zone," +
                " gen_col as extract(year from ts) not null" +
                ") with (number_of_replicas=0)");
        ensureYellow();
        assertThrowsMatches(() -> execute("insert into generated_column (id, gen_col) values (1, null)"),
                     isSQLError(is("\"gen_col\" must not be null"), INTERNAL_ERROR, BAD_REQUEST, 4000));
    }

    @Test
    public void testInsertFromSubQueryWithGeneratedColumns() {
        execute("create table source_table (" +
                " id integer," +
                " ts timestamp with time zone" +
                ") with (number_of_replicas=0)");
        execute("create table target_table (" +
                " id integer," +
                " ts timestamp with time zone," +
                " day as date_trunc('day', ts)" +
                ") with (number_of_replicas=0)");
        ensureYellow();

        execute("insert into source_table (id, ts) values (?, ?)", new Object[]{
            1, "2015-11-18T11:11:00"});
        refresh();

        execute("insert into target_table (id, ts) (select id, ts from source_table)");
        refresh();

        execute("select day from target_table");
        assertThat(response.rows()[0][0], is(1447804800000L));
    }

    @Test
    public void testInsertIntoPartitionedTableFromSubQueryWithGeneratedColumns() {
        execute("create table source_table (" +
                " id integer," +
                " ts timestamp with time zone" +
                ") with (number_of_replicas=0)");
        execute("create table target_table (" +
                " id integer," +
                " ts timestamp with time zone," +
                " day as date_trunc('day', ts)" +
                ") partitioned by (day) with (number_of_replicas=0)");
        ensureYellow();

        execute("insert into source_table (id, ts) values (?, ?)", new Object[]{
            1, "2015-11-18T11:11:00"});
        refresh();

        execute("insert into target_table (id, ts) (select id, ts from source_table)");
        refresh();

        execute("select day from target_table");
        assertThat(response.rows()[0][0], is(1447804800000L));
    }

    @Test
    public void testInsertFromSubQueryInvalidGeneratedColumnValue() {
        execute("create table source_table (" +
                " id integer," +
                " ts timestamp with time zone," +
                " day timestamp with time zone" +
                ") with (number_of_replicas=0)");
        execute("create table target_table (" +
                " id integer," +
                " ts timestamp with time zone," +
                " day as date_trunc('day', ts)" +
                ") with (number_of_replicas=0)");
        ensureYellow();

        execute("insert into source_table (id, ts, day) values (?, ?, ?)", new Object[]{
            1, "2015-11-18T11:11:00", "2015-11-18T11:11:00"});
        refresh();

        // will fail because `day` column has invalid value at source table
        execute("insert into target_table (id, ts, day) (select id, ts, day from source_table)");
        assertThat(response.rowCount(), is(0L));
    }

    @Test
    public void testInsertIntoGeneratedPartitionedColumnValueGiven() throws Exception {
        execute("create table export(col1 integer, col2 int)");
        execute("create table import (\n" +
                "  col1 int, \n" +
                "  col2 int, \n" +
                "  gen_new as (col1 + col2)" +
                ") partitioned by (gen_new)");
        ensureYellow();
        execute("insert into export (col1, col2) values (1, 2)");
        refresh();
        execute("insert into import (col1, col2, gen_new) (select col1, col2, col1+col2 from export)");
        refresh();
        execute("select gen_new from import");
        assertThat(response.rows()[0][0], is(3));
    }

    @Test
    public void testInsertGeneratedPrimaryKeyValue() throws Exception {
        execute("create table test(col1 as 3 * col2 primary key, col2 integer)");
        ensureYellow();
        execute("insert into test (col2) values (1)");
        refresh();
        execute("select col1 from test");
        assertThat(response.rows()[0][0], is(3));
    }

    @Test
    public void testInsertGeneratedPartitionedPrimaryKey() throws Exception {
        execute("create table test(col1 integer primary key, col2 as 2 * col1 primary key) " +
                "partitioned by (col2)");
        ensureYellow();
        execute("insert into test (col1) values(1)");
        refresh();
        execute("select col2 from test");
        assertThat(response.rows()[0][0], is(2));
    }

    @Test
    public void testInsertGeneratedPrimaryKeyValueGiven() throws Exception {
        execute("create table test(col1 integer primary key, col2 as col1 + 3 primary key)");
        execute("insert into test(col1, col2) values (1, 4)");
        refresh();
        execute("select col2 from test");
        assertThat(response.rows()[0][0], is(4));

        assertThrowsMatches(
            () -> execute("insert into test(col1, col2) values (1, 0)"),
            isSQLError(
                is("Given value 0 for generated column col2 does not match calculation (col1 + 3) = 4"),
                INTERNAL_ERROR,
                BAD_REQUEST,
                4000
            )
        );

        execute("refresh table test");
        execute("select count(*) from test");
        assertThat("Second insert was rejected", response.rows()[0][0], is(1L));
    }

    @Test
    public void testInsertFromSubQueryMissingPrimaryKeyValues() throws Exception {
        execute("create table source(col1 integer)");
        execute("create table target(col1 integer primary key, col2 integer primary key)");
        ensureYellow();
        execute("insert into source (col1) values (1)");
        refresh();
        assertThrowsMatches(() -> execute("insert into target (col1) (select col1 from source)"),
                     isSQLError(containsString("Column \"col2\" is required but is missing from the insert statement"),
                                INTERNAL_ERROR, BAD_REQUEST, 4000));
    }

    @Test
    public void testInsertFromSubQueryWithNotNullConstraint() throws Exception {
        execute("create table source(col1 integer, col2 integer)");
        execute("create table target(col1 integer primary key, col2 integer not null)");
        ensureYellow();
        execute("insert into source (col1) values (1)");
        refresh();

        execute("insert into target (col1, col2) (select col1, col2 from source)");
        assertEquals(0, response.rowCount());
    }

    @Test
    public void testInsertFromSubQueryWithNotNullConstraintColumnAbsent() throws Exception {
        execute("create table source(col1 integer)");
        execute("create table target(col1 integer primary key, col2 integer not null)");
        ensureYellow();
        execute("insert into source (col1) values (1)");
        refresh();

        execute("insert into target (col1) (select col1 from source)");
        assertEquals(0, response.rowCount());
    }

    @Test
    public void testInsertFromSubQueryWithNotNullConstraintAndGeneratedColumns() {
        execute("create table source(id int, ts timestamp with time zone)");
        execute("create table target (" +
                " id int primary key," +
                " ts timestamp with time zone," +
                " gen_col as extract(year from ts) not null" +
                ") with (number_of_replicas=0)");
        ensureYellow();

        execute("insert into source (id) values (1)");
        refresh();

        execute("insert into target (id, ts) (select id, ts from source)");
        assertEquals(0, response.rowCount());
    }

    @Test
    public void testInsertFromSubQueryGeneratedPrimaryKey() throws Exception {
        execute("create table source(col1 integer)");
        execute("create table target(col1 integer primary key)");
        ensureYellow();
        execute("insert into source (col1) values (1)");
        refresh();
        execute("insert into target (col1) (select col1 from source)");
        refresh();
        execute("select col1 from target");
        assertThat((Integer) response.rows()[0][0], is(1));
    }

    @Test
    public void testGeneratedColumnAsPrimaryKeyValueEvaluateToNull() throws Exception {
        execute("CREATE TABLE test (col1 TEXT, col2 AS try_cast(col1 AS INT) PRIMARY KEY)");
        assertThrowsMatches(() -> execute("insert into test (col1) values ('a')"),
                     isSQLError(is("Primary key value must not be NULL"), INTERNAL_ERROR, BAD_REQUEST, 4000));
    }

    @Test
    public void testDynamicTimestampIntegrationTest() throws Exception {
        execute("create table dyn_ts (id integer primary key) with (column_policy = 'dynamic')");
        ensureYellow();
        execute("insert into dyn_ts (id, ts) values (0, '2015-01-01')");
        refresh();
        waitForMappingUpdateOnAll("dyn_ts", "ts");
        execute("insert into dyn_ts (id, ts) values (1, '2015-02-01')");
        // string is not converted to timestamp
        execute("select data_type from information_schema.columns where table_name='dyn_ts' and column_name='ts'");
        assertThat(response.rows()[0][0], is("text"));

        execute("select _raw from dyn_ts where id = 0");
        assertThat((String) response.rows()[0][0], is("{\"id\":0,\"ts\":\"2015-01-01\"}"));
    }

    @Test
    public void testInsertFromQueryWithGeneratedPrimaryKey() throws Exception {
        execute("create table t (x int, y int, z as x + y primary key)");
        ensureYellow();
        execute("insert into t (x, y) (select 1, 2 from sys.cluster)");
        assertThat(response.rowCount(), is(1L));
        assertThat(execute("select * from t where z = 3").rowCount(), is(1L));
    }

    @Test
    public void testInsertIntoTableWithNestedPrimaryKeyFromQuery() throws Exception {
        execute("create table t (o object as (ot object as (x int primary key)))");
        ensureYellow();
        assertThat(execute("insert into t (o) (select {ot={x=10}} from sys.cluster)").rowCount(), is(1L));
        assertThat(execute("select * from t where o['ot']['x'] = 10").rowCount(), is(1L));
    }

    @Test
    public void testInsertIntoTableWithNestedPartitionedByFromQuery() throws Exception {
        execute("create table t (o object as (x int)) partitioned by (o['x'])");
        ensureYellow();
        assertThat(execute("insert into t (o) (select {x=10} from sys.cluster)").rowCount(), is(1L));
    }

    /**
     * Test that when an error happens on the primary, the record should never be inserted on the replica.
     * Since we cannot force a select statement to be executed on a replica, we repeat this test to increase the chance.
     */
    @Repeat(iterations = 5)
    @Test
    public void testInsertWithErrorMustNotBeInsertedOnReplica() throws Exception {
        execute("create table test (id integer primary key, name string) with (number_of_replicas=1)");
        ensureYellow();
        execute("insert into test (id, name) values (1, 'foo')");
        assertThat(response.rowCount(), is(1L));
        assertThrowsMatches(() -> execute("insert into test (id, name) values (1, 'bar')"),
                            isSQLError(containsString("A document with the same primary key exists already"),
                                       UNIQUE_VIOLATION,
                                       CONFLICT,
                                       4091));
        refresh();
        // we want to read from the replica but cannot force it, lets select twice to increase chances
        execute("select _version, name from test");
        assertThat((String) response.rows()[0][1], is("foo"));
        assertThat((Long) response.rows()[0][0], is(1L));
        execute("select _version, name from test");
        assertThat((String) response.rows()[0][1], is("foo"));
        assertThat((Long) response.rows()[0][0], is(1L));
    }

    @Test
    public void testInsertIntoFromSystemTable() {
        execute("create table shard_stats (" +
                "   log_date timestamp with time zone," +
                "   shard_id string," +
                "   num_docs long)" +
                " clustered into 1 shards with (number_of_replicas=0)");
        execute("insert into shard_stats (log_date, shard_id, num_docs) (select CURRENT_TIMESTAMP as log_date, id, num_docs from sys.shards)");
        refresh();
        execute("select * from shard_stats");
        assertThat(response.rowCount(), is(1L));
    }

    @Test
    public void testInsertWithNestedGeneratedColumn() {
        execute("create table t (x int, obj object as (y as x + 1, z int))");
        execute("insert into t (x, obj) values (10, {z=4})");
        execute("refresh table t");
        execute("select x, obj['y'], obj['z'] from t");
        assertThat(printedTable(response.rows()), is("10| 11| 4\n"));
    }

    @Test
    public void testInsertIntoTablePartitionedOnGeneratedColumnBasedOnColumnWithinObject() {
        execute("create table t (" +
                "   day as date_trunc('day', obj['ts'])," +
                "   obj object (strict) as (" +
                "       ts timestamp with time zone" +
                "   )" +
                ") partitioned by (day) clustered into 1 shards");
        execute("insert into t (obj) (select {ts=1549966541034})");
        execute("refresh table t");
        assertThat(
            printedTable(execute("select day, obj from t").rows()),
            is("1549929600000| {ts=1549966541034}\n")
        );
    }

    @Test
    public void testInsertIntoPartitionedTableFromPartitionedTable() {
        execute("create table tsrc (country string not null, name string not null) " +
                "partitioned by (country) " +
                "clustered into 1 shards");
        execute("insert into tsrc (country, name) values ('AR', 'Felipe')");
        execute("refresh table tsrc");

        execute("create table tdst (country string not null, name string not null) " +
                "partitioned by (country) " +
                "clustered into 1 shards");

        execute("insert into tdst (country, name) (select country, name from tsrc)");
        assertThat(response.rowCount(), is(1L));
    }

    @Test
    public void testInsertDefaultExpressions() {
        execute("create table t (" +
                " id int," +
                " owner text default 'crate'" +
                ") with (number_of_replicas=0)");

        execute("insert into t (id) values (?)",
                new Object[]{1});
        execute("insert into t (id) select 2");
        execute("insert into t(id) values (?), (?)",
                new Object[]{3, 4});
        execute("insert into t (id, owner) select 5, 'cr8'");
        execute("refresh table t");

        assertThat(
            printedTable(execute("select * from t order by id").rows()),
            is("1| crate\n" +
               "2| crate\n" +
               "3| crate\n" +
               "4| crate\n" +
               "5| cr8\n")
        );
    }

    @Test
    public void test_insert_with_id_in_returning_clause() {
        execute("create table t (" +
                " id int," +
                " owner text default 'crate'" +
                ") with (number_of_replicas=0)");

        execute("insert into t (id) values (?) returning id",
                new Object[]{1});

        assertThat(response.rowCount(), is(1L));
        assertThat(response.cols()[0], is("id"));
        assertThat(response.rows()[0][0], is(1));

    }

    @Test
    public void test_insert_with_multiple_values_in_returning_clause() {
        execute("create table t (" +
                " id int," +
                " owner text default 'crate'" +
                ") with (number_of_replicas=0)");

        execute("insert into t (id) values (?) returning id, owner",
                new Object[]{1});

        assertThat(response.rowCount(), is(1L));
        assertThat(response.cols()[0], is("id"));
        assertThat(response.cols()[1], is("owner"));
        assertThat(response.rows()[0][0], is(1));
        assertThat(response.rows()[0][1], is("crate"));

    }

    @Test
    public void test_insert_with_function_in_returning_clause() {
        execute("create table t (" +
                " id int," +
                " owner text default 'crate'" +
                ") with (number_of_replicas=0)");

        execute("insert into t (id) values (?) returning id + 1 as bar",
                new Object[]{1});

        assertThat(response.rowCount(), is(1L));
        assertThat(response.cols()[0], is("bar"));
        assertThat(response.rows()[0][0], is(2));

    }


    @Test
    public void test_insert_with_seq_no_in_returning_clause() {
        execute("create table t (" +
                " id int," +
                " owner text default 'crate'" +
                ") with (number_of_replicas=0)");

        execute("insert into t (id) values (?) returning _seq_no as seq",
                new Object[]{1});

        assertThat(response.rowCount(), is(1L));
        assertThat(response.cols()[0], is("seq"));
        assertThat(response.rows()[0][0], is(0L));

    }

    @Test
    public void test_insert_with_owner_renamed_in_returning_clause() {
        execute("create table t (" +
                " id int," +
                " owner text default 'crate'" +
                ") with (number_of_replicas=0)");

        execute("insert into t (id) values (?) returning owner as name",
                new Object[]{1});

        assertThat(response.rowCount(), is(1L));
        assertThat(response.cols()[0], is("name"));
        assertThat(response.rows()[0][0], is("crate"));

    }

    @Test
    public void test_insert_from_subquery_with_id_field_in_returning_clause() {
        execute("create table t (" +
                " id int," +
                " owner text default 'crate'" +
                ") with (number_of_replicas=0)");

        execute("insert into t (id)  select '1' as id returning id as foo, owner as name");

        assertThat(response.rowCount(), is(1L));
        assertThat(response.cols()[0], is("foo"));
        assertThat(response.rows()[0][0], is(1));
        assertThat(response.cols()[1], is("name"));
        assertThat(response.rows()[0][1], is("crate"));

    }

    @Test
    public void test_insert_from_values_on_duplicate_key_with_id_in_returning_clause() {
        execute("create table t1 (id integer primary key, other string) clustered into 1 shards");
        execute("insert into t1 (id, other) values (1, 'test'), (2, 'test2')");

        execute("insert into t1 (id, other) values (1, 'updated') ON CONFLICT (id) DO UPDATE SET other = 'updated' returning id, other");
        assertThat(response.rowCount(), is(1L));
        refresh();

        assertThat(response.cols()[0], is("id"));
        assertThat(response.rows()[0][0], is(1));

        assertThat(response.cols()[1], is("other"));
        assertThat(response.rows()[0][1], is("updated"));

        execute("insert into t1 (id, other) values (1, 'updated_again'), (3, 'new') ON CONFLICT (id) DO UPDATE SET other = excluded.other returning id, other");
        assertThat(response.rowCount(), is(2L));
        refresh();

        assertThat(response.cols()[0], is("id"));
        assertThat(response.cols()[1], is("other"));

        assertThat(response.rows()[0][0], is(1));
        assertThat(response.rows()[0][1], is("updated_again"));

        assertThat(response.rows()[1][0], is(3));
        assertThat(response.rows()[1][1], is("new"));
    }

    @Test
    public void test_insert_from_values_and_subquery_into_varchar_with_length_column() {
        execute("CREATE TABLE t1 (str varchar(3)) CLUSTERED INTO 1 SHARDS");

        execute("INSERT INTO t1 (str) VALUES ('abc'), ('abc   ')");
        execute("INSERT INTO t1 (str) (SELECT UNNEST(['bcd', 'bcd   ']))");
        execute("REFRESH TABLE t1");

        assertThat(
            printedTable(execute("SELECT * FROM t1 ORDER BY str").rows()),
            is("abc\n" +
               "abc\n" +
               "bcd\n" +
               "bcd\n"));
    }

    @Test
    public void test_insert_from_query_with_limit_and_parameter_placeholders() throws Exception {
        execute("create table target (catalog_id string, user_id string, user_order int)");
        execute("create table source (id string)");
        execute(
            "INSERT INTO target (catalog_id, user_id, user_order) " +
            "SELECT id, ?, 0 from source LIMIT ?",
            new Object[] { 7, 12 }
        );
    }

    @Test
    public void test_can_insert_into_object_column_using_json_cast() throws Exception {
        execute("create table tbl (obj object(dynamic) as (x int, y int))");
        execute("insert into tbl (obj) values (?::json)", new Object[] { "{\"x\": 10, \"y\": 20}" });
        assertThat(response.rowCount(), is(1L));
        execute("refresh table tbl");
        execute("select obj, obj::json from tbl");

        assertThat(printedTable(response.rows()), Matchers.oneOf(
            "{x=10, y=20}| {x=10, y=20}\n",
            "{x=10, y=20}| {\"x\":10,\"y\":20}\n"
        ));
    }

    @Test
    public void test_insert_into_table_with_primary_key_with_default_clause_sets_right_id() {
        execute("create table tbl (id text default gen_random_text_uuid() primary key, x int)");
        execute("insert into tbl (x) values (1)");
        execute("refresh table tbl");
        execute("select _id, id from tbl");
        assertThat("_id and id must match", response.rows()[0][0], is(response.rows()[0][1]));

        String id = (String) response.rows()[0][1];
        execute("select x from tbl where id = ?", new Object[] { id });
        assertThat(printedTable(response.rows()), is(
            "1\n"
        ));
    }
}
