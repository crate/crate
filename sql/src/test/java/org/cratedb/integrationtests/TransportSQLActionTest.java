package org.cratedb.integrationtests;

import org.cratedb.action.sql.SQLAction;
import org.cratedb.action.sql.SQLRequest;
import org.cratedb.action.sql.SQLResponse;
import org.cratedb.sql.DuplicateKeyException;
import org.cratedb.sql.SQLParseException;
import org.cratedb.test.integration.AbstractSharedCrateClusterTest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class TransportSQLActionTest extends AbstractSharedCrateClusterTest {

    private SQLResponse response;
    private SQLRequest request;

    @Override
    protected int numberOfNodes() {
        return 1;
    }

    private void execute(String stmt, Object[] args) {
        request = new SQLRequest(stmt, args);
        response = client().execute(SQLAction.INSTANCE, new SQLRequest(stmt, args)).actionGet();
    }

    private void execute(String stmt) {
        request = new SQLRequest(stmt);
        response = client().execute(SQLAction.INSTANCE, new SQLRequest(stmt)).actionGet();
    }

    @Test
    public void testSelectKeepsOrder() throws Exception {
        createIndex("test");
        client().prepareIndex("test", "default", "id1").setSource("{}").execute().actionGet();
        refresh();
        execute("select \"_id\" as b, \"_version\" as a from test");
        assertArrayEquals(new String[]{"b", "a"}, response.cols());
        assertEquals(1, response.rows().length);
    }

    @Test
    public void testSelectStar() throws Exception {
        prepareCreate("test")
                .addMapping("default",
                    "firstName", "type=string",
                    "lastName", "type=string")
                .execute().actionGet();
        execute("select * from test");
        assertArrayEquals(new String[]{"firstName", "lastName"}, response.cols());
        assertEquals(0, response.rows().length);
    }

    @Test
    public void testSelectStarWithOther() throws Exception {
        prepareCreate("test")
                .addMapping("default",
                        "firstName", "type=string",
                        "lastName", "type=string")
                .execute().actionGet();
        client().prepareIndex("test", "default", "id1").setRefresh(true)
                .setSource("{\"firstName\":\"Youri\",\"lastName\":\"Zoon\"}")
                .execute().actionGet();
        execute("select \"_version\", *, \"_id\" from test");
        assertArrayEquals(new String[]{"_version", "firstName", "lastName", "_id"},
                response.cols());
        assertEquals(1, response.rows().length);
        assertArrayEquals(new Object[]{1L, "Youri", "Zoon", "id1"}, response.rows()[0]);
    }

    @Test
    public void testSelectWithParams() throws Exception {
        prepareCreate("test")
            .addMapping("default",
                "first_name", "type=string,index=not_analyzed",
                "last_name", "type=string,index=not_analyzed",
                "age", "type=double,index=not_analyzed")
            .execute().actionGet();
        client().prepareIndex("test", "default", "id1").setRefresh(true)
            .setSource("{\"first_name\":\"Youri\",\"last_name\":\"Zoon\", \"age\": 38}")
            .execute().actionGet();

        Object[] args = new Object[] {"id1"};
        execute("select first_name, last_name from test where \"_id\" = $1", args);
        assertArrayEquals(new Object[]{"Youri", "Zoon"}, response.rows()[0]);

        args = new Object[] {"Zoon"};
        execute("select first_name, last_name from test where last_name = $1", args);
        assertArrayEquals(new Object[]{"Youri", "Zoon"}, response.rows()[0]);

        args = new Object[] {38, "Zoon"};
        execute("select first_name, last_name from test where age = $1 and last_name = $2", args);
        assertArrayEquals(new Object[]{"Youri", "Zoon"}, response.rows()[0]);

        args = new Object[] {38, "Zoon"};
        execute("select first_name, last_name from test where age = ? and last_name = ?", args);
        assertArrayEquals(new Object[]{"Youri", "Zoon"}, response.rows()[0]);
    }

    @Test
    public void testSelectStarWithOtherAndAlias() throws Exception {
        prepareCreate("test")
                .addMapping("default",
                        "firstName", "type=string",
                        "lastName", "type=string")
                .execute().actionGet();
        client().prepareIndex("test", "default", "id1").setRefresh(true)
                .setSource("{\"firstName\":\"Youri\",\"lastName\":\"Zoon\"}")
                .execute().actionGet();
        execute("select *, \"_version\", \"_version\" as v from test");
        assertArrayEquals(new String[]{"firstName", "lastName", "_version", "v"},
                response.cols());
        assertEquals(1, response.rows().length);
        assertArrayEquals(new Object[]{"Youri", "Zoon", 1L, 1L}, response.rows()[0]);
    }


    @Test
    public void testSelectNestedColumns() throws Exception {
        prepareCreate("test")
                .addMapping("default",
                        "message", "type=string",
                        "person", "type=object")
                .execute().actionGet();
        client().prepareIndex("test", "default", "id1").setRefresh(true)
                .setSource("{\"message\":\"I'm addicted to kite\", " +
                        "\"person\": { \"name\": \"Youri\", \"addresses\": [ { \"city\": " +
                        "\"Dirksland\", \"country\": \"NL\" } ] }}")
                .execute().actionGet();

        execute("select message, person['name'], person['addresses']['city'] from test " +
                "where person['name'] = 'Youri'");

        assertArrayEquals(new String[]{"message", "person['name']", "person['addresses']['city']"},
                response.cols());
        assertEquals(1, response.rows().length);
        assertArrayEquals(new Object[]{"I'm addicted to kite", "Youri",
                new ArrayList<String>(){{add("Dirksland");}}},
                response.rows()[0]);
    }

    @Test
    public void testFilterByEmptyString() throws Exception {
        prepareCreate("test")
                .addMapping("default",
                        "name", "type=string,index=not_analyzed")
                .execute().actionGet();
        client().prepareIndex("test", "default", "id1").setRefresh(true)
                .setSource("{\"name\":\"\"}")
                .execute().actionGet();
        client().prepareIndex("test", "default", "id2").setRefresh(true)
                .setSource("{\"name\":\"Ruben Lenten\"}")
                .execute().actionGet();

        execute("select name from test where name = ''");
        assertEquals(1, response.rows().length);
        assertEquals("", response.rows()[0][0]);

        execute("select name from test where name != ''");
        assertEquals(1, response.rows().length);
        assertEquals("Ruben Lenten", response.rows()[0][0]);

    }

    @Test
    public void testFilterByNull() throws Exception {
        prepareCreate("test")
                .addMapping("default",
                        "name", "type=string,index=not_analyzed")
                .execute().actionGet();
        client().prepareIndex("test", "default", "id1").setRefresh(true)
                .setSource("{}")
                .execute().actionGet();
        client().prepareIndex("test", "default", "id2").setRefresh(true)
                .setSource("{\"name\":\"Ruben Lenten\"}")
                .execute().actionGet();
        client().prepareIndex("test", "default", "id3").setRefresh(true)
                .setSource("{\"name\":\"\"}")
                .execute().actionGet();

        execute("select \"_id\" from test where name is null");
        assertEquals(1, response.rows().length);
        assertEquals("id1", response.rows()[0][0]);

        execute("select \"_id\" from test where name is not null order by \"_uid\"");
        assertEquals(2, response.rows().length);
        assertEquals("id2", response.rows()[0][0]);

        // missing field is null
        execute("select \"_id\" from test where invalid is null");
        assertEquals(3, response.rows().length);

        execute("select name from test where name is not null and name!=''");
        assertEquals(1, response.rows().length);
        assertEquals("Ruben Lenten", response.rows()[0][0]);

    }


    /**
     * Queries are case sensitive by default, however column names without quotes are converted
     * to lowercase which is the same behaviour as in postgres
     * see also http://www.thenextage.com/wordpress/postgresql-case-sensitivity-part-1-the-ddl/
     *
     * @throws Exception
     */
    @Test
    public void testColsAreCaseSensitive() throws Exception {
        prepareCreate("test")
                .addMapping("default",
                        "firstName", "type=string,store=true,index=not_analyzed",
                        "firstname", "type=string,store=true,index=not_analyzed")
                .execute().actionGet();

        client().prepareIndex("test", "default", "id1").setRefresh(true)
                .setSource("{\"firstname\":\"LowerCase\",\"firstName\":\"CamelCase\"}")
                .execute().actionGet();

        execute(
                "select FIRSTNAME, \"firstname\", \"firstName\" from test");
        assertArrayEquals(new String[]{"firstname", "firstname", "firstName"}, response.cols());
        assertEquals(1, response.rows().length);
        assertEquals("LowerCase", response.rows()[0][0]);
        assertEquals("LowerCase", response.rows()[0][1]);
        assertEquals("CamelCase", response.rows()[0][2]);
    }


    @Test
    public void testIdSelectWithResult() throws Exception {
        createIndex("test");
        client().prepareIndex("test", "default", "id1").setSource("{}").execute().actionGet();
        refresh();
        execute("select \"_id\" from test");
        assertArrayEquals(new String[]{"_id"}, response.cols());
        assertEquals(1, response.rows().length);
        assertEquals(1, response.rows()[0].length);
        assertEquals("id1", response.rows()[0][0]);
    }

    @Test
    public void testDelete() throws Exception {
        createIndex("test");
        client().prepareIndex("test", "default", "id1").setSource("{}").execute().actionGet();
        refresh();
        execute("delete from test");
        assertEquals(0, response.rows().length);
        execute("select \"_id\" from test");
        assertEquals(0, response.rows().length);
    }

    @Test
    public void testDeleteWithWhere() throws Exception {
        createIndex("test");
        client().prepareIndex("test", "default", "id1").setSource("{}").execute().actionGet();
        client().prepareIndex("test", "default", "id2").setSource("{}").execute().actionGet();
        client().prepareIndex("test", "default", "id3").setSource("{}").execute().actionGet();
        refresh();
        execute("delete from test where \"_id\" = 'id1'");
        assertEquals(0, response.rows().length);
        execute("select \"_id\" from test");
        assertEquals(2, response.rows().length);
    }

    @Test
    public void testSelectSource() throws Exception {
        createIndex("test");
        client().prepareIndex("test", "default", "id1").setSource("{\"a\":1}")
                .execute().actionGet();
        refresh();
        execute("select \"_source\" from test");
        assertArrayEquals(new String[]{"_source"}, response.cols());
        assertEquals(1, response.rows().length);
        assertEquals(1, response.rows()[0].length);
        assertEquals(1, (long) ((Map<String, Integer>) response.rows()[0][0]).get("a"));
    }

    @Test
    public void testSelectObject() throws Exception {
        createIndex("test");
        client().prepareIndex("test", "default", "id1").setSource("{\"a\":{\"nested\":2}}")
                .execute().actionGet();
        refresh();
        execute("select a from test");
        assertArrayEquals(new String[]{"a"}, response.cols());
        assertEquals(1, response.rows().length);
        assertEquals(1, response.rows()[0].length);
        assertEquals(2, (long) ((Map<String, Integer>) response.rows()[0][0]).get("nested"));
    }


    @Test
    public void testSqlRequestWithLimit() throws Exception {
        createIndex("test");
        client().prepareIndex("test", "default", "id1").setSource("{}").execute().actionGet();
        client().prepareIndex("test", "default", "id2").setSource("{}").execute().actionGet();
        refresh();
        execute("select \"_id\" from test limit 1");
        assertEquals(1, response.rows().length);
    }


    @Test
    public void testSqlRequestWithLimitAndOffset() throws Exception {
        createIndex("test");
        client().prepareIndex("test", "default", "id1").setSource("{}").execute().actionGet();
        client().prepareIndex("test", "default", "id2").setSource("{}").execute().actionGet();
        client().prepareIndex("test", "default", "id3").setSource("{}").execute().actionGet();
        refresh();
        execute("select \"_id\" from test limit 1 offset 1");
        assertEquals(1, response.rows().length);
    }


    @Test
    public void testSqlRequestWithFilter() throws Exception {
        createIndex("test");
        client().prepareIndex("test", "default", "id1").setSource("{}").execute().actionGet();
        client().prepareIndex("test", "default", "id2").setSource("{}").execute().actionGet();
        refresh();
        execute("select \"_id\" from test where \"_id\"='id1'");
        assertEquals(1, response.rows().length);
        assertEquals("id1", response.rows()[0][0]);
    }

    @Test
    public void testSqlRequestWithNotEqual() throws Exception {
        createIndex("test");
        client().prepareIndex("test", "default", "id1").setSource("{}").execute().actionGet();
        client().prepareIndex("test", "default", "id2").setSource("{}").execute().actionGet();
        refresh();
        execute("select \"_id\" from test where \"_id\"!='id1'");
        assertEquals(1, response.rows().length);
        assertEquals("id2", response.rows()[0][0]);
    }


    @Test
    public void testSqlRequestWithOneOrFilter() throws Exception {
        createIndex("test");
        client().prepareIndex("test", "default", "id1").setSource("{}").execute().actionGet();
        client().prepareIndex("test", "default", "id2").setSource("{}").execute().actionGet();
        client().prepareIndex("test", "default", "id3").setSource("{}").execute().actionGet();
        refresh();
        execute(
                "select \"_id\" from test where \"_id\"='id1' or \"_id\"='id3' order by " +
                        "\"_uid\"");
        assertEquals(2, response.rows().length);
        assertEquals("id1", response.rows()[0][0]);
        assertEquals("id3", response.rows()[1][0]);
    }

    @Test
    public void testSqlRequestWithOneMultipleOrFilter() throws Exception {
        createIndex("test");
        client().prepareIndex("test", "default", "id1").setSource("{}").execute().actionGet();
        client().prepareIndex("test", "default", "id2").setSource("{}").execute().actionGet();
        client().prepareIndex("test", "default", "id3").setSource("{}").execute().actionGet();
        client().prepareIndex("test", "default", "id4").setSource("{}").execute().actionGet();
        refresh();
        execute(
                "select \"_id\" from test where " +
                        "\"_id\"='id1' or \"_id\"='id2' or \"_id\"='id4' " +
                        "order by \"_uid\"");
        assertEquals(3, response.rows().length);
        System.out.println(Arrays.toString(response.rows()[0]));
        System.out.println(Arrays.toString(response.rows()[1]));
        System.out.println(Arrays.toString(response.rows()[2]));
        assertEquals("id1", response.rows()[0][0]);
        assertEquals("id2", response.rows()[1][0]);
        assertEquals("id4", response.rows()[2][0]);
    }

    @Test
    public void testSqlRequestWithDateFilter() throws Exception {
        prepareCreate("test")
                .addMapping("default",
                        "date", "type=date")
                .execute().actionGet();
        client().prepareIndex("test", "default", "id1")
                .setSource("{\"date\":\"2013-10-01\"}")
                .execute().actionGet();
        client().prepareIndex("test", "default", "id2")
                .setSource("{\"date\":\"2013-10-02\"}")
                .execute().actionGet();
        refresh();
        execute(
                "select date from test where date = '2013-10-01'");
        assertEquals(1, response.rows().length);
        assertEquals("2013-10-01", response.rows()[0][0]);
    }

    @Test
    public void testSqlRequestWithDateGtFilter() throws Exception {
        prepareCreate("test")
                .addMapping("default",
                        "date", "type=date")
                .execute().actionGet();
        client().prepareIndex("test", "default", "id1")
                .setSource("{\"date\":\"2013-10-01\"}")
                .execute().actionGet();
        client().prepareIndex("test", "default", "id2")
                .setSource("{\"date\":\"2013-10-02\"}")
                .execute().actionGet();
        refresh();
        execute(
                "select date from test where date > '2013-10-01'");
        assertEquals(1, response.rows().length);
        assertEquals("2013-10-02", response.rows()[0][0]);
    }

    @Test
    public void testSqlRequestWithNumericGtFilter() throws Exception {
        prepareCreate("test")
                .addMapping("default",
                        "i", "type=long")
                .execute().actionGet();
        client().prepareIndex("test", "default", "id1")
                .setSource("{\"i\":10}")
                .execute().actionGet();
        client().prepareIndex("test", "default", "id2")
                .setSource("{\"i\":20}")
                .execute().actionGet();
        refresh();
        execute(
                "select i from test where i > 10");
        assertEquals(1, response.rows().length);
        assertEquals(20, response.rows()[0][0]);
    }


    @Test
    public void testInsertWithColumnNames() throws Exception {
//            ESLogger logger = Loggers.getLogger("org.elasticsearch.org.cratedb");
//            logger.setLevel("DEBUG");
        prepareCreate("test")
                .addMapping("default",
                        "firstName", "type=string,store=true,index=not_analyzed",
                        "lastName", "type=string,store=true,index=not_analyzed")
                .execute().actionGet();

        execute("insert into test (\"firstName\", \"lastName\") values('Youri', 'Zoon')");
        refresh();

        execute("select * from test where \"firstName\" = 'Youri'");

        assertEquals(1, response.rows().length);
        assertEquals("Youri", response.rows()[0][0]);
        assertEquals("Zoon", response.rows()[0][1]);
    }

    @Test
    public void testInsertWithoutColumnNames() throws Exception {
        prepareCreate("test")
                .addMapping("default",
                        "firstName", "type=string,store=true,index=not_analyzed",
                        "lastName", "type=string,store=true,index=not_analyzed")
                .execute().actionGet();

        execute("insert into test values('Youri', 'Zoon')");
        refresh();

        execute("select * from test where \"firstName\" = 'Youri'");

        assertEquals(1, response.rows().length);
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

        execute("insert into test values(true, '2013-09-10T21:51:43', 1.79769313486231570e+308, 3.402, 2147483647, 9223372036854775807, 32767, 'Youri')");
        refresh();


        execute("select * from test");

        assertEquals(1, response.rows().length);
        assertEquals(true, response.rows()[0][0]);
        assertEquals(1378849903000L, response.rows()[0][1]);
        assertEquals(1.79769313486231570e+308, response.rows()[0][2]);
        assertEquals(3.402, response.rows()[0][3]);
        assertEquals(2147483647, response.rows()[0][4]);
        assertEquals(9223372036854775807L, response.rows()[0][5]);
        assertEquals(32767, response.rows()[0][6]);
        assertEquals("Youri", response.rows()[0][7]);
    }

    @Test
    public void testInsertMultipleRows() throws Exception {
        prepareCreate("test")
                .addMapping("default",
                        "age", "type=integer",
                        "name", "type=string,store=true,index=not_analyzed")
                .execute().actionGet();

        execute("insert into test values(32, 'Youri'), (42, 'Ruben')");
        refresh();

        execute("select * from test order by \"name\"");

        assertEquals(2, response.rows().length);
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

        Object[] args = new Object[] {32, "Youri"};
        execute("insert into test values(?, ?)", args);
        refresh();

        execute("select * from test where name = 'Youri'");

        assertEquals(1, response.rows().length);
        assertEquals(32, response.rows()[0][0]);
        assertEquals("Youri", response.rows()[0][1]);
    }

    @Test
    public void testInsertMultipleRowsWithParams() throws Exception {
        prepareCreate("test")
                .addMapping("default",
                        "age", "type=integer",
                        "name", "type=string,store=true,index=not_analyzed")
                .execute().actionGet();

        Object[] args = new Object[] {32, "Youri", 42, "Ruben"};
        execute("insert into test values(?, ?), (?, ?)", args);
        refresh();

        execute("select * from test order by \"name\"");

        assertEquals(2, response.rows().length);
        assertArrayEquals(new Object[]{42, "Ruben"}, response.rows()[0]);
        assertArrayEquals(new Object[]{32, "Youri"}, response.rows()[1]);
    }

    @Test
    public void testInsertObject() throws Exception {
        prepareCreate("test")
                .addMapping("default",
                        "message", "type=string,store=true,index=not_analyzed",
                        "person", "type=object,store=true")
                .execute().actionGet();

        Map<String, String> person = new HashMap<String, String>();
        person.put("first_name", "Youri");
        person.put("last_name", "Zoon");
        Object[] args = new Object[] {"I'm addicted to kite", person};

        execute("insert into test values(?, ?)", args);
        refresh();

        execute("select * from test");

        assertEquals(1, response.rows().length);
        assertArrayEquals(args, response.rows()[0]);
    }

    @Test
    public void testUpdateObject() throws Exception {
        prepareCreate("test")
                .addMapping("default",
                        "message", "type=string,index=not_analyzed")
                .execute().actionGet();

        execute("insert into test values('hello'),('again')");
        refresh();

        execute("update test set message='b' where message = 'hello'");

        assertEquals(1, response.rowCount());
        refresh();

        execute("select message from test where message='b'");
        assertEquals(1, response.rows().length);
        assertEquals("b", response.rows()[0][0]);

    }

    @Test
    public void testUpdateObjectWithArgs() throws Exception {
        prepareCreate("test")
                .addMapping("default",
                        "coolness", "type=float,index=not_analyzed")
                .execute().actionGet();

        execute("insert into test values(1.1),(2.2)");
        refresh();

        execute("update test set coolness=3.3 where coolness = ?", new Object[]{2.2});

        assertEquals(1, response.rowCount());
        refresh();

        execute("select coolness from test where coolness=3.3");
        assertEquals(1, response.rows().length);
        assertEquals(3.3, response.rows()[0][0]);

    }


    @Test
    public void testInsertWithPrimaryKey() throws Exception {
        createTestIndexWithPkMapping();

        Object[] args = new Object[] { "1",
            "A towel is about the most massively useful thing an interstellar hitch hiker can have."};
        execute("insert into test (pk_col, message) values (?, ?)", args);
        refresh();

        GetResponse response = client().prepareGet("test", "default", "1").execute().actionGet();
        assertTrue(response.getSourceAsMap().containsKey("message"));
    }

    @Test
    public void testInsertWithPrimaryKeyMultiValues() throws Exception {
        createTestIndexWithPkMapping();

        Object[] args = new Object[] {
            "1", "All the doors in this spaceship have a cheerful and sunny disposition.",
            "2", "I always thought something was fundamentally wrong with the universe"
        };
        execute("insert into test (pk_col, message) values (?, ?), (?, ?)", args);
        refresh();

        GetResponse response = client().prepareGet("test", "default", "1").execute().actionGet();
        assertTrue(response.getSourceAsMap().containsKey("message"));
    }

    @Test (expected = DuplicateKeyException.class)
    public void testInsertWithUniqueContraintViolation() throws Exception {
        createTestIndexWithPkMapping();

        Object[] args = new Object[] {
            "1", "All the doors in this spaceship have a cheerful and sunny disposition.",
        };
        execute("insert into test (pk_col, message) values (?, ?)", args);

        args = new Object[] {
            "1", "I always thought something was fundamentally wrong with the universe"
        };

        execute("insert into test (pk_col, message) values (?, ?)", args);
    }

    private void createTestIndexWithPkMapping() {
        Settings settings = settingsBuilder()
            .put("crate.primary_keys", "pk_col").build();

        prepareCreate("test")
            .setSettings(settings)
            .addMapping("default",
                "pk_col", "type=string,store=true,index=not_analyzed",
                "message", "type=string,store=true,index=not_analyzed")
                .execute().actionGet();
    }

    @Test (expected = SQLParseException.class)
    public void testMultiplePrimaryKeyColumns() throws Exception {
        Settings settings = settingsBuilder()
            .put("crate.primary_keys", "pk_col1, pk_col2").build();

        prepareCreate("test")
            .setSettings(settings)
            .addMapping("default",
                "pk_col", "type=string,store=true,index=not_analyzed",
                "message", "type=string,store=true,index=not_analyzed")
            .execute().actionGet();

        Object[] args = new Object[] {
            "Life, loathe it or ignore it, you can't like it."
        };
        execute("insert into test (message) values (?)", args);
    }

    @Test (expected = SQLParseException.class)
    public void testInsertWithPKMissingOnInsert() throws Exception {
        createTestIndexWithPkMapping();

        Object[] args = new Object[] {
            "In the beginning the Universe was created.\n" +
                "This has made a lot of people very angry and been widely regarded as a bad move."
        };
        execute("insert into test (message) values (?)", args);
    }
}
