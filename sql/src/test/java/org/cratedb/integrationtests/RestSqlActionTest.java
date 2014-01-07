package org.cratedb.integrationtests;

import org.codehaus.jackson.map.ObjectMapper;
import org.cratedb.SQLTransportIntegrationTest;
import org.cratedb.action.sql.parser.SQLXContentSourceContext;
import org.cratedb.action.sql.parser.SQLXContentSourceParser;
import org.elasticsearch.common.bytes.BytesArray;
import org.junit.Before;
import org.junit.Test;
import org.skyscreamer.jsonassert.JSONAssert;

public class RestSqlActionTest extends SQLTransportIntegrationTest {

    @Before
    public void setUpIndex() throws Exception {
        new Setup(this).setUpLocations();
    }

    @Test
    public void testSqlRequest() throws Exception {
        String json = restSQLExecute("{\"stmt\": \"select * from locations where id = '1'\"}");
        JSONAssert.assertEquals(
                "{\n" +
                        "  \"cols\" : [ \"date\", \"description\", \"id\", \"kind\", \"name\", " +
                        "\"position\", \"race\" ],\n" +
                        "  \"rows\" : [ [ 308534400000, " +
                        "\"Relative to life on NowWhat, living on an affluent world in the North" +
                        " West ripple of the Galaxy is said to be easier by a factor of about " +
                        "seventeen million.\", \"1\", \"Galaxy\", \"North West Ripple\", 1, " +
                        "null ] ],\n" +
                        " \"rowcount\": 1," +
                        " \"duration\": " + responseDuration +
                        "}"
                , json, false);
    }


    @Test
    public void testSqlRequestWithArgs() throws Exception {

        String json = restSQLExecute("{\n" +
                "    \"stmt\": \"select * from locations where id = $2\",\n" +
                "    \"args\": [[\"1\", \"2\"], \"1\", 1, 2, 2.0, 99999999999999999999999999999999]\n" +
                "}\n");
        JSONAssert.assertEquals(
            "{\n" +
                "  \"cols\" : [ \"date\", \"description\", \"id\", \"kind\", \"name\", " +
                "\"position\", \"race\" ],\n" +
                "  \"rows\" : [ [ 308534400000, " +
                "\"Relative to life on NowWhat, living on an affluent world in the North" +
                " West ripple of the Galaxy is said to be easier by a factor of about " +
                "seventeen million.\", \"1\", \"Galaxy\", \"North West Ripple\", 1, null ] ],\n" +
                " \"rowcount\": 1," +
                " \"duration\": " + responseDuration +
                "}"
            , json, false);
    }

    @Test
    public void testSqlRequestWithNullArgs() throws Exception {

        String json = restSQLExecute("{\n" +
                "    \"stmt\": \"insert into locations (id, name, kind) values (?, ?)\",\n" +
                "    \"args\": [\"100\", \"Somewhere\", null]\n" +
                "}\n");

        JSONAssert.assertEquals(
            "{\n" +
                "  \"cols\" : [ ],\n" +
                "  \"rows\" : [ ],\n" +
                "  \"rowcount\" : 1,\n" +
                "  \"duration\" : \n" + responseDuration +
                "}", json, false);
    }

    @Test
    public void testArgsParser() throws Exception {
        SQLXContentSourceContext context = new SQLXContentSourceContext();
        SQLXContentSourceParser parser = new SQLXContentSourceParser(context);
        BytesArray source = new BytesArray("{\n" +
            "    \"stmt\": \"select * from locations where id = $2\",\n" +
            "    \"args\": [[\"1\", \"2\"], \"1\", 1, 2, 2.0, 99999999999999999999999999999999]\n" +
            "}\n");
        parser.parseSource(source);

        ObjectMapper mapper = new ObjectMapper();
        assertEquals(
            "[[\"1\",\"2\"],\"1\",1,2,2.0,99999999999999999999999999999999]",
            mapper.writeValueAsString(context.args()));
    }

    @Test
    public void testArgsParserNestedList() throws Exception {

        SQLXContentSourceContext context = new SQLXContentSourceContext();
        SQLXContentSourceParser parser = new SQLXContentSourceParser(context);
        BytesArray source = new BytesArray("{\n" +
            "    \"stmt\": \"select * from locations where id = $2\",\n" +
            "    \"args\": [[\"1\", \"2\", [\"1\"]], \"1\", 1, 2, 2.0, 99999999999999999999999999999999]\n" +
            "}\n");
        parser.parseSource(source);
        ObjectMapper mapper = new ObjectMapper();
        assertEquals("[[\"1\",\"2\",[\"1\"]],\"1\",1,2,2.0,99999999999999999999999999999999]",
            mapper.writeValueAsString(context.args()));
    }

    @Test
    public void testArgsParserNestedMap() throws Exception {

        SQLXContentSourceContext context = new SQLXContentSourceContext();
        SQLXContentSourceParser parser = new SQLXContentSourceParser(context);
        BytesArray source = new BytesArray("{\n" +
            "    \"stmt\": \"select * from locations where id = $2\",\n" +
            "    \"args\": [{\"1\": \"2\"}, 1]\n" +
            "}\n");
        parser.parseSource(source);
        ObjectMapper mapper = new ObjectMapper();
        assertEquals("[{\"1\":\"2\"},1]", mapper.writeValueAsString(context.args()));
    }

    @Test
    public void testArgsParserVeryNestedMap() throws Exception {

        SQLXContentSourceContext context = new SQLXContentSourceContext();
        SQLXContentSourceParser parser = new SQLXContentSourceParser(context);
        BytesArray source = new BytesArray("{\n" +
            "    \"stmt\": \"select * from locations\",\n" +
            "    \"args\": [{\"1\": {\"2\": {\"3\": 3}}}, [{\"1\": {\"2\": [2, 2]}}]]\n" +
            "}\n");
        parser.parseSource(source);
        ObjectMapper mapper = new ObjectMapper();
        assertEquals("[{\"1\":{\"2\":{\"3\":3}}},[{\"1\":{\"2\":[2,2]}}]]",
            mapper.writeValueAsString(context.args()));
    }
}
