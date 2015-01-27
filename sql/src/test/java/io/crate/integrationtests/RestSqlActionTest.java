/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

import io.crate.action.sql.parser.SQLXContentSourceContext;
import io.crate.action.sql.parser.SQLXContentSourceParser;
import io.crate.test.integration.CrateIntegrationTest;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.common.bytes.BytesArray;
import org.junit.Before;
import org.junit.Test;
import org.skyscreamer.jsonassert.JSONAssert;

@CrateIntegrationTest.ClusterScope(scope = CrateIntegrationTest.Scope.GLOBAL)
public class RestSqlActionTest extends SQLTransportIntegrationTest {

    private static final String LN = System.getProperty("line.separator");

    @Before
    public void setUpIndex() throws Exception {
        new Setup(sqlExecutor).setUpLocations();
    }

    @Test
    public void testSqlRequest() throws Exception {
        String json = restSQLExecute("{\"stmt\": \"select * from locations where id = '1'\"}");
        JSONAssert.assertEquals(
                "{" + LN +
                        "  \"cols\" : [ \"date\", \"description\", \"id\", \"kind\", \"name\", " +
                        "\"position\", \"race\" ]," + LN +
                        "  \"rows\" : [ [ 308534400000, " +
                        "\"Relative to life on NowWhat, living on an affluent world in the North" +
                        " West ripple of the Galaxy is said to be easier by a factor of about " +
                        "seventeen million.\", \"1\", \"Galaxy\", \"North West Ripple\", 1, " +
                        "null ] ]," + LN +
                        " \"rowcount\": 1," +
                        " \"duration\": " + responseDuration +
                        "}"
                , json, true
        );
    }

    @Test
    public void testSqlRequestIncludeTypes() throws Exception {
        String json = restSQLExecute("{\"stmt\": \"select * from locations where id = '1'\"}", true);
        JSONAssert.assertEquals(
                "{" + LN +
                        "  \"cols\" : [ \"date\", \"description\", \"id\", \"kind\", \"name\", " +
                        "\"position\", \"race\" ]," + LN +
                        "  \"col_types\" : [ 11, 4, 4, 4, 4, 9, 12 ]," + LN +
                        "  \"rows\" : [ [ 308534400000, " +
                        "\"Relative to life on NowWhat, living on an affluent world in the North" +
                        " West ripple of the Galaxy is said to be easier by a factor of about " +
                        "seventeen million.\", \"1\", \"Galaxy\", \"North West Ripple\", 1, " +
                        "null ] ]," + LN +
                        " \"rowcount\": 1," +
                        " \"duration\": " + responseDuration +
                        "}"
                , json, true);
    }


    @Test
    public void testSqlRequestWithArgs() throws Exception {

        String json = restSQLExecute("{" + LN +
                "    \"stmt\": \"select * from locations where id = $2\"," + LN +
                "    \"args\": [[\"1\", \"2\"], \"1\", 1, 2, 2.0, 99999999999999999999999999999999]" + LN +
                "}" + LN);
        JSONAssert.assertEquals(
            "{" + LN +
                "  \"cols\" : [ \"date\", \"description\", \"id\", \"kind\", \"name\", " +
                "\"position\", \"race\" ]," + LN +
                "  \"rows\" : [ [ 308534400000, " +
                "\"Relative to life on NowWhat, living on an affluent world in the North" +
                " West ripple of the Galaxy is said to be easier by a factor of about " +
                "seventeen million.\", \"1\", \"Galaxy\", \"North West Ripple\", 1, null ] ]," + LN +
                " \"rowcount\": 1," +
                " \"duration\": " + responseDuration +
                "}"
            , json, true);
    }

    @Test
    public void testSqlRequestWithNullArgs() throws Exception {

        String json = restSQLExecute("{" + LN +
                "    \"stmt\": \"insert into locations (id, name, kind) values (?, ?, ?)\"," + LN +
                "    \"args\": [\"100\", \"Somewhere\", null]" + LN +
                "}" + LN);

        JSONAssert.assertEquals(
            "{" + LN +
                "  \"cols\" : [ ]," + LN +
                "  \"rows\" : [ ]," + LN +
                "  \"rowcount\" : 1," + LN +
                "  \"duration\" : " + LN + responseDuration +
                "}", json, true);
    }

    @Test
    public void testArgsParser() throws Exception {
        SQLXContentSourceContext context = new SQLXContentSourceContext();
        SQLXContentSourceParser parser = new SQLXContentSourceParser(context);
        BytesArray source = new BytesArray("{" + LN +
            "    \"stmt\": \"select * from locations where id = $2\"," + LN +
            "    \"args\": [[\"1\", \"2\"], \"1\", 1, 2, 2.0, 99999999999999999999999999999999]" + LN +
            "}" + LN);
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
        BytesArray source = new BytesArray("{" + LN +
            "    \"stmt\": \"select * from locations where id = $2\"," + LN +
            "    \"args\": [[\"1\", \"2\", [\"1\"]], \"1\", 1, 2, 2.0, 99999999999999999999999999999999]" + LN +
            "}" + LN);
        parser.parseSource(source);
        ObjectMapper mapper = new ObjectMapper();
        assertEquals("[[\"1\",\"2\",[\"1\"]],\"1\",1,2,2.0,99999999999999999999999999999999]",
            mapper.writeValueAsString(context.args()));
    }

    @Test
    public void testArgsParserNestedMap() throws Exception {

        SQLXContentSourceContext context = new SQLXContentSourceContext();
        SQLXContentSourceParser parser = new SQLXContentSourceParser(context);
        BytesArray source = new BytesArray("{" + LN +
            "    \"stmt\": \"select * from locations where id = $2\"," + LN +
            "    \"args\": [{\"1\": \"2\"}, 1]" + LN +
            "}" + LN);
        parser.parseSource(source);
        ObjectMapper mapper = new ObjectMapper();
        assertEquals("[{\"1\":\"2\"},1]", mapper.writeValueAsString(context.args()));
    }

    @Test
    public void testArgsParserVeryNestedMap() throws Exception {

        SQLXContentSourceContext context = new SQLXContentSourceContext();
        SQLXContentSourceParser parser = new SQLXContentSourceParser(context);
        BytesArray source = new BytesArray("{" + LN +
            "    \"stmt\": \"select * from locations\"," + LN +
            "    \"args\": [{\"1\": {\"2\": {\"3\": 3}}}, [{\"1\": {\"2\": [2, 2]}}]]" + LN +
            "}" + LN);
        parser.parseSource(source);
        ObjectMapper mapper = new ObjectMapper();
        assertEquals("[{\"1\":{\"2\":{\"3\":3}}},[{\"1\":{\"2\":[2,2]}}]]",
            mapper.writeValueAsString(context.args()));
    }

    @Test
    public void testSqlRequestWithBulkArgs() throws Exception {

        String json = restSQLExecute("{" + LN +
                "    \"stmt\": \"insert into locations (id, name, kind) values (?, ?, ?)\"," + LN +
                "    \"bulk_args\": [[\"200\", \"Somewhere\", \"planet\"], [\"201\", \"Somewhere else town\", \"city\"]]" + LN +
                "}" + LN);

        JSONAssert.assertEquals(
                "{" + LN +
                        "  \"cols\" : [ ]," + LN +
                        "  \"results\" : [ {\"rowcount\" : 1 }, {\"rowcount\": 1 } ]," + LN +
                        "  \"duration\" : " + LN + responseDuration +
                        "}", json, true);
    }

}
