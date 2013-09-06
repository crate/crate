package org.cratedb.integrationtests;

import org.cratedb.action.sql.SQLAction;
import org.cratedb.action.sql.SQLRequest;
import org.cratedb.action.sql.SQLResponse;
import org.cratedb.test.integration.AbstractSharedCrateClusterTest;
import org.junit.Test;

import java.util.Arrays;
import java.util.Map;

public class TransportSQLActionTest extends AbstractSharedCrateClusterTest {

    private SQLResponse response;
    private SQLRequest request;

    @Override
    protected int numberOfNodes() {
        return 1;
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

}
