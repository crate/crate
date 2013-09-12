package org.cratedb.integrationtests;

import org.cratedb.action.sql.SQLRequestBuilder;
import org.cratedb.action.sql.SQLResponse;
import org.cratedb.action.sql.parser.SQLXContentSourceContext;
import org.cratedb.action.sql.parser.SQLXContentSourceParser;
import org.cratedb.sql.SQLParseException;
import org.cratedb.test.integration.AbstractSharedCrateClusterTest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Before;
import org.junit.Test;
import org.skyscreamer.jsonassert.JSONAssert;

import java.io.IOException;
import java.util.Arrays;

public class RestSqlActionTest extends AbstractSharedCrateClusterTest {


    private static XContentBuilder builder;

    static {
        try {
            builder = XContentFactory.contentBuilder(XContentType.JSON);
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File
            // Templates.
        }
    }

    @Override
    protected int numberOfNodes() {
        return 1;
    }

    @Before
    public void setUpIndex() throws Exception {
        new Setup(this).setUpLocations();
    }

    private String sql(String source) throws IOException {
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        builder.generator().usePrettyPrint();
        SQLRequestBuilder requestBuilder = new SQLRequestBuilder(client());
        requestBuilder.source(new BytesArray(source));
        SQLResponse response = requestBuilder.execute().actionGet();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        return builder.string();
    }

    @Test
    public void testSqlRequest() throws Exception {
        String json = sql("{\"stmt\": \"select * from locations where \\\"_id\\\" = '1'\"}");
        //System.out.println(json);
        JSONAssert.assertEquals(
                "{\n" +
                        "  \"cols\" : [ \"date\", \"description\", \"kind\", \"name\", " +
                        "\"position\" ],\n" +
                        "  \"rows\" : [ [ \"1979-10-12T00:00:00.000Z\", " +
                        "\"Relative to life on NowWhat, living on an affluent world in the North" +
                        " West ripple of the Galaxy is said to be easier by a factor of about " +
                        "seventeen million.\", \"Galaxy\", \"North West Ripple\", 1 ] ]\n" +
                        "}"
                , json, true);
    }


    @Test
    public void testSqlRequestWithArgs() throws Exception {

        String json = sql("{\n" +
            "    \"stmt\": \"select * from locations where \\\"_id\\\" = $2\",\n" +
            "    \"args\": [[\"1\", \"2\"], \"1\", 1, 2, 2.0, 99999999999999999999999999999999]\n" +
            "}\n");
        JSONAssert.assertEquals(
            "{\n" +
                "  \"cols\" : [ \"date\", \"description\", \"kind\", \"name\", " +
                "\"position\" ],\n" +
                "  \"rows\" : [ [ \"1979-10-12T00:00:00.000Z\", " +
                "\"Relative to life on NowWhat, living on an affluent world in the North" +
                " West ripple of the Galaxy is said to be easier by a factor of about " +
                "seventeen million.\", \"Galaxy\", \"North West Ripple\", 1 ] ]\n" +
                "}"
            , json, true);
    }


    @Test
    public void testArgsParser() throws Exception {
        SQLXContentSourceContext context = new SQLXContentSourceContext();
        SQLXContentSourceParser parser = new SQLXContentSourceParser(context);
        BytesArray source = new BytesArray("{\n" +
            "    \"stmt\": \"select * from locations where \\\"_id\\\" = $2\",\n" +
            "    \"args\": [[\"1\", \"2\"], \"1\", 1, 2, 2.0, 99999999999999999999999999999999]\n" +
            "}\n");
        parser.parseSource(source);

        assertEquals("[[\"1\", \"2\"], \"1\", 1, 2, 2.0, 99999999999999999999999999999999]", arrayRepr(context.args()));
    }

    @Test(expected = SQLParseException.class)
    public void testArgsParserNestedList() throws Exception {

        // nested lists are not supported

        SQLXContentSourceContext context = new SQLXContentSourceContext();
        SQLXContentSourceParser parser = new SQLXContentSourceParser(context);
        BytesArray source = new BytesArray("{\n" +
            "    \"stmt\": \"select * from locations where \\\"_id\\\" = $2\",\n" +
            "    \"args\": [[\"1\", \"2\", [\"1\"]], \"1\", 1, 2, 2.0, 99999999999999999999999999999999]\n" +
            "}\n");
        parser.parseSource(source);
    }

    @Test(expected = SQLParseException.class)
    public void testArgsParserNestedMap() throws Exception {

        // nested maps are not supported

        SQLXContentSourceContext context = new SQLXContentSourceContext();
        SQLXContentSourceParser parser = new SQLXContentSourceParser(context);
        BytesArray source = new BytesArray("{\n" +
            "    \"stmt\": \"select * from locations where \\\"_id\\\" = $2\",\n" +
            "    \"args\": [{\"1\": \"2\"}, 1]\n" +
            "}\n");
        parser.parseSource(source);
    }

    /**
     * method to print an object[]
     * similar to Arrays.deepToString() but quotes strings
     * @param array
     * @return
     */
    private String arrayRepr(Object[] array) {
        StringBuilder sb = new StringBuilder();
        sb.append("[");

        for (int i = 0; i < array.length; i++) {
            if (array[i] instanceof Object[]) {
                sb.append(arrayRepr((Object[])array[i]));
            } else if (array[i] instanceof String) {
                sb.append("\"" + array[i].toString() + "\"");
            } else {
                sb.append(array[i].toString());
            }

            if (i + 1 < array.length) {
                sb.append(", ");
            }
        }

        sb.append("]");
        return sb.toString();
    }

}
