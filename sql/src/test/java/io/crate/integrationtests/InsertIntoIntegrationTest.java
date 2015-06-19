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

package io.crate.integrationtests;

import io.crate.action.sql.SQLActionException;
import io.crate.action.sql.SQLBulkResponse;
import io.crate.testing.TestingHelpers;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.hamcrest.core.IsNull;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.*;
import static org.hamcrest.core.Is.is;

@ElasticsearchIntegrationTest.ClusterScope(numDataNodes = 2, numClientNodes = 0, randomDynamicTemplates = false)
public class InsertIntoIntegrationTest extends SQLTransportIntegrationTest {

    private Setup setup = new Setup(sqlExecutor);

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testInsertWithColumnNames() throws Exception {
        prepareCreate("test")
                .addMapping("default",
                        "firstName", "type=string,store=true,index=not_analyzed",
                        "lastName", "type=string,store=true,index=not_analyzed")
                .execute().actionGet();
        ensureGreen();
        execute("insert into test (\"firstName\", \"lastName\") values('Youri', 'Zoon')");
        assertEquals(1, response.rowCount());
        assertThat(response.duration(), greaterThanOrEqualTo(0L));
        refresh();

        execute("select * from test where \"firstName\" = 'Youri'");

        assertEquals(1, response.rowCount());
        assertEquals("Youri", response.rows()[0][0]);
        assertEquals("Zoon", response.rows()[0][1]);
    }

    @Test
    public void testInsertWithoutColumnNames() throws Exception {
        execute("create table test (\"firstName\" string, \"lastName\" string)");
        ensureGreen();
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
        prepareCreate("test")
                .addMapping("default",
                        "boolean", "type=boolean",
                        "datetime", "type=date",
                        "double", "type=double",
                        "float", "type=float",
                        "integer", "type=integer",
                        "long", "type=long",
                        "short", "type=short",
                        "string", "type=string,index=not_analyzed")
                .execute().actionGet();
        ensureGreen();

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
        assertEquals((short)32767, response.rows()[1][6]);
        assertEquals("Youri", response.rows()[1][7]);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testInsertCoreTypesAsArray() throws Exception {
        execute("create table test (" +
                        "\"boolean\" array(boolean), " +
                        "\"datetime\" array(timestamp), " +
                        "\"double\" array(double), " +
                        "\"float\" array(float), " +
                        "\"integer\" array(integer), " +
                        "\"long\" array(long), " +
                        "\"short\" array(short), " +
                        "\"string\" array(string) " +
                        ") with (number_of_replicas=0)"
        );
        ensureGreen();

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
        assertEquals(true, ((Object[]) response.rows()[0][0])[0]);
        assertEquals(false, ((Object[]) response.rows()[0][0])[1]);

        assertThat(((Long) ((Object[]) response.rows()[0][1])[0]), is(1378849903000L));
        assertThat(((Long) ((Object[]) response.rows()[0][1])[1]), is(1384120303000L));

        assertThat(((Double) ((Object[]) response.rows()[0][2])[0]), is(1.79769313486231570e+308));
        assertThat(((Double) ((Object[]) response.rows()[0][2])[1]), is(1.69769313486231570e+308));


        assertEquals(3.402f, ((Number) ((Object[]) response.rows()[0][3])[0]).floatValue(), 0.002f);
        assertEquals(3.403f, ((Number) ((Object[]) response.rows()[0][3])[1]).floatValue(), 0.002f);
        assertThat(((Object[]) response.rows()[0][3])[2], nullValue());

        assertThat(((Integer) ((Object[]) response.rows()[0][4])[0]), is(2147483647));
        assertThat(((Integer) ((Object[]) response.rows()[0][4])[1]), is(234583));

        assertThat(((Long) ((Object[]) response.rows()[0][5])[0]), is(9223372036854775807L));
        assertThat(((Long) ((Object[]) response.rows()[0][5])[1]), is(4L));

        assertThat(((Short) ((Object[]) response.rows()[0][6])[0]), is((short) 32767));
        assertThat(((Short) ((Object[]) response.rows()[0][6])[1]), is((short) 2));

        assertThat(((String) ((Object[]) response.rows()[0][7])[0]), is("Youri"));
        assertThat(((String) ((Object[]) response.rows()[0][7])[1]), is("Juri"));
    }

    @Test
    public void testInsertBadIpAdress() throws Exception {
        execute("create table t (i ip) with (number_of_replicas=0)");
        ensureGreen();
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("Failed to validate ip [192.168.1.500], not a valid ipv4 address");
        execute("insert into t (i) values ('192.168.1.2'), ('192.168.1.3'),('192.168.1.500')");
    }

    @Test
    public void testInsertMultipleRows() throws Exception {
        prepareCreate("test")
                .addMapping("default",
                        "age", "type=integer",
                        "name", "type=string,store=true,index=not_analyzed")
                .execute().actionGet();
        ensureGreen();

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
        prepareCreate("test")
                .addMapping("default",
                        "age", "type=integer",
                        "name", "type=string,store=true,index=not_analyzed")
                .execute().actionGet();
        ensureGreen();

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
        ensureGreen();
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
        ensureGreen();

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
        ensureGreen();

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
    public void testInsertEmptyObjectArray() throws Exception {
        execute("create table test (" +
                "  id integer primary key," +
                "  details array(object)" +
                ")");
        ensureGreen();
        execute("insert into test (id, details) values (?, ?)", new Object[]{1, new Map[0]});
        refresh();
        execute("select id, details from test");
        assertEquals(1, response.rowCount());
        assertEquals(1, response.rows()[0][0]);

        assertThat(response.rows()[0][1], instanceOf(Object[].class));
        Object[] details = ((Object[]) response.rows()[0][1]);
        assertThat(details.length, is(0));
    }

    @Test
    public void testInsertWithPrimaryKey() throws Exception {
        this.setup.createTestTableWithPrimaryKey();

        Object[] args = new Object[]{"1",
                "A towel is about the most massively useful thing an interstellar hitch hiker can have."};
        execute("insert into test (pk_col, message) values (?, ?)", args);
        refresh();

        GetResponse response = client().prepareGet("test", "default", "1").execute().actionGet();
        assertTrue(response.getSourceAsMap().containsKey("message"));
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

        GetResponse response = client().prepareGet("test", "default", "1").execute().actionGet();
        assertTrue(response.getSourceAsMap().containsKey("message"));
    }

    @Test
    public void testInsertWithUniqueConstraintViolation() throws Exception {
        this.setup.createTestTableWithPrimaryKey();

        Object[] args = new Object[]{
                "1", "All the doors in this spaceship have a cheerful and sunny disposition.",
        };
        execute("insert into test (pk_col, message) values (?, ?)", args);

        args = new Object[]{
                "1", "I always thought something was fundamentally wrong with the universe"
        };

        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("A document with the same primary key exists already");

        execute("insert into test (pk_col, message) values (?, ?)", args);
    }

    @Test
    public void testInsertWithPKMissingOnInsert() throws Exception {
        this.setup.createTestTableWithPrimaryKey();

        Object[] args = new Object[]{
                "In the beginning the Universe was created.\n" +
                        "This has made a lot of people very angry and been widely regarded as a bad move."
        };

        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("Primary key is required but is missing from the insert statement");

        execute("insert into test (message) values (?)", args);
    }

    @Test
    public void testInsertWithClusteredByNull() throws Exception {
        execute("create table quotes (id integer, quote string) clustered by(id) " +
                "with (number_of_replicas=0)");
        ensureGreen();

        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("Clustered by value must not be NULL");
        execute("insert into quotes (id, quote) values(?, ?)",
                new Object[]{null, "I'd far rather be happy than right any day."});
    }

    @Test
    public void testInsertWithClusteredByWithoutValue() throws Exception {
        execute("create table quotes (id integer, quote string) clustered by(id) " +
                "with (number_of_replicas=0)");
        ensureGreen();

        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("Clustered by value is required but is missing from the insert statement");
        execute("insert into quotes (quote) values(?)",
                new Object[]{"I'd far rather be happy than right any day."});
    }



    @Test
    public void testInsertFromQueryWithSysColumn() throws Exception {
        execute("create table target (name string, a string, b string) clustered into 1 shards with (number_of_replicas = 0)");
        execute("create table source (name string) clustered into 1 shards with (number_of_replicas = 0)");
        ensureYellow();

        execute("insert into source (name) values ('yalla')");
        execute("refresh table source");

        execute("insert into target (name, a, b) (select name, _raw, _id from source)");
        execute("refresh table target");

        execute("select name, a, b from target");
        assertThat((String) response.rows()[0][0], is("yalla"));
        assertThat((String) response.rows()[0][1], is("{\"name\":\"yalla\"}"));
        assertThat((String) response.rows()[0][2], IsNull.notNullValue());
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
    public void testInsertArrayLiteralFirstNull() throws Exception {
        execute("create table users(id int primary key, friends array(string), name string)");
        ensureYellow();
        execute("insert into users (id, friends, name) values (0, [null, 'gedöns'], 'björk')");
        execute("insert into users (id, friends, name) values (1, [null], null)");
        execute("refresh table users");
        execute("select friends, name from users order by id");
        assertThat(response.rowCount(), is(2L));

        Object[] friends = (Object[]) response.rows()[0][0];
        assertThat(friends[0], nullValue());
        assertThat(((String) friends[1]), is("gedöns"));
        assertThat((String)response.rows()[0][1], is("björk"));

        friends = ((Object[]) response.rows()[1][0]);
        assertNull(friends[0]);
    }

    @Test
    public void testInsertFromQueryGlobalAggregate() throws Exception {
        this.setup.setUpLocations();
        execute("refresh table locations");

        execute("create table aggs (" +
                " c long," +
                " s double" +
                ") with (number_of_replicas=0)");
        ensureGreen();

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
        ensureGreen();

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
                " date timestamp," +
                " kind string," +
                " position long," +         // <<-- original type is integer, testing implicit cast
                " description string," +
                " race object," +
                " index name_description_ft using fulltext(name, description) with (analyzer='english')" +
                ") clustered by(id) into 2 shards with(number_of_replicas=0)");

        execute("insert into locations2 (select * from locations)");
        assertThat(response.rowCount(), is(13L));

        execute("refresh table locations2");
        execute("select * from locations2 order by id");
        assertThat(response.rowCount(), is(13L));

        for (int i = 0; i < rowsOriginal.length; i++) {
            rowsOriginal[i][5] =  (long)((int)rowsOriginal[i][5]);
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
    public void testInsertFromQueryWithAggregateWithinScalarFunction() throws Exception {
        this.setup.setUpCharacters();
        waitNoPendingTasksOnAll();
        execute("create table t (count int, id int) with (number_of_replicas=0)");
        ensureGreen();
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
        assertThat((int)response.rows()[3][1], is(4));
    }

    @Test
    public void testInsertFromSubQueryNonDistributedGroupBy() throws Exception {
        execute("create table nodes (count integer, name string) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into nodes (count, name) (select count(*), name from sys.nodes group by name)");
        refresh();
        execute("select count, name from nodes order by name");
        assertThat(response.rowCount(), is(2L));
        assertThat((int)response.rows()[0][0], is(1));
        assertThat((String)response.rows()[0][1], is("node_s0"));
        assertThat((int)response.rows()[1][0], is(1));
        assertThat((String)response.rows()[1][1], is("node_s1"));
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
        assertThat((int)response.rows()[3][0], is(4));
        assertThat((String)response.rows()[3][1], is("Arthur"));

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
        assertThat((Long)response.rows()[0][1], is(2L));
        assertThat((Long)response.rows()[0][1], is(2L));

        // set all 'female' values to true
        execute("insert into t (id, name, female) (select id, name, female from characters) " +
                "on duplicate key update female = ?",
                new Object[]{ true });
        assertThat(response.rowCount(), is(4L));
        refresh();

        execute("select female, count(*) from t group by female");
        assertThat(response.rowCount(), is(1L));
        assertThat((Long)response.rows()[0][1], is(4L));

        // set all 'female' values back to their original values
        execute("insert into t (id, name, female) (select id, name, female from characters) " +
                        "on duplicate key update female = values(female)",
                new Object[]{ true });
        assertThat(response.rowCount(), is(4L));
        refresh();

        execute("select female, count(*) from t group by female order by female");
        assertThat(response.rowCount(), is(2L));
        assertThat((Long)response.rows()[0][1], is(2L));
        assertThat((Long) response.rows()[0][1], is(2L));
    }

    @Test
    public void testInsertFromSubQueryWithVersion() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("\"_version\" column is not valid in the WHERE clause");
        execute("create table users (name string)");
        ensureYellow();
        execute("insert into users (name) (select name from users where _version = 1)");
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
        ensureGreen();
        execute("insert into custom.source (name, zipcode, city) values (?, ?, ?)", new Object[][]{
                {"Schulz", "10243", "Berlin"},
                {"Dings", "14713", "Leipzig"},
                {"Foo", "10243", "Musterhausen"}
        });
        ensureGreen();
        refresh();

        execute("select table_name, schema_name, partition_ident, values, number_of_shards, number_of_replicas "+
                "from information_schema.table_partitions where schema_name='custom' and table_name='source' order by partition_ident");
        String[] rows = TestingHelpers.printedTable(response.rows()).split("\n");
        assertThat(rows[0], is("source| custom| 043k4pbidhkms| {city=Berlin}| 5| 0"));
        assertThat(rows[1], is("source| custom| 0444opb9e1t6ipo| {city=Leipzig}| 5| 0"));
        assertThat(rows[2], is("source| custom| 046kqtbjehin4q31elpmarg| {city=Musterhausen}| 5| 0"));

        execute("insert into custom.destination (select * from custom.source)");
        assertThat(response.rowCount(), is(3L));
        ensureGreen();
        refresh();

        execute("select * from custom.destination order by city");
        assertThat(TestingHelpers.printedTable(response.rows()), is(
                "Berlin| Schulz| 10243\n" +
                "Leipzig| Dings| 14713\n" +
                "Musterhausen| Foo| 10243\n"));

        execute("select table_name, schema_name, partition_ident, values, number_of_shards, number_of_replicas "+
                "from information_schema.table_partitions where schema_name='custom' and table_name='destination' order by partition_ident");
        rows = TestingHelpers.printedTable(response.rows()).split("\n");
        assertThat(rows[0], is("destination| custom| 04332c1i6gpg| {zipcode=10243}| 5| 0"));
        assertThat(rows[1], is("destination| custom| 04332d1n64pg| {zipcode=14713}| 5| 0"));

    }

    @Test
    public void testBulkInsert() throws Exception {
        execute("create table giveittome (" +
                "  date timestamp," +
                "  dirty_names array(string)," +
                "  lashes short primary key" +
                ") with (number_of_replicas=0)");
        ensureYellow();
        int bulkSize = randomIntBetween(1, 2000);
        Object[][] bulkArgs = new Object[bulkSize][];
        for (int i = 0; i < bulkSize; i++) {
            bulkArgs[i] = new Object[] { System.currentTimeMillis() + i, new String[]{randomAsciiOfLength(5), randomAsciiOfLength(2)}, (short)i };
        }
        SQLBulkResponse bulkResponse = execute("insert into giveittome (date, dirty_names, lashes) values (?, ?, ?)", bulkArgs);
        assertThat(bulkResponse.results().length, is(bulkSize));
        execute("refresh table giveittome");
        // assert that bulk insert has inserted everything it said it has
        execute("select sum(lashes), date from giveittome group by date");
        assertThat(response.rowCount(), is((long)bulkSize));
    }
}
