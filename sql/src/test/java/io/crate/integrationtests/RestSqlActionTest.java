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
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.common.bytes.BytesArray;
import org.junit.Before;
import org.junit.Test;
import org.skyscreamer.jsonassert.JSONAssert;

public class RestSqlActionTest extends SQLTransportIntegrationTest {

    @Before
    public void setUpIndex() throws Exception {
        new Setup(sqlExecutor).setUpLocations();
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
                , json, true
        );
    }

    @Test
    public void testSqlRequestIncludeTypes() throws Exception {
        String json = restSQLExecute("{\"stmt\": \"select * from locations where id = '1'\"}", true);
        JSONAssert.assertEquals(
                "{\n" +
                        "  \"cols\" : [ \"date\", \"description\", \"id\", \"kind\", \"name\", " +
                        "\"position\", \"race\" ],\n" +
                        "  \"col_types\" : [ 11, 4, 4, 4, 4, 9, 12 ],\n" +
                        "  \"rows\" : [ [ 308534400000, " +
                        "\"Relative to life on NowWhat, living on an affluent world in the North" +
                        " West ripple of the Galaxy is said to be easier by a factor of about " +
                        "seventeen million.\", \"1\", \"Galaxy\", \"North West Ripple\", 1, " +
                        "null ] ],\n" +
                        " \"rowcount\": 1," +
                        " \"duration\": " + responseDuration +
                        "}"
                , json, true);
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
            , json, true);
    }

    @Test
    public void testSqlRequestWithNullArgs() throws Exception {

        String json = restSQLExecute("{\n" +
                "    \"stmt\": \"insert into locations (id, name, kind) values (?, ?, ?)\",\n" +
                "    \"args\": [\"100\", \"Somewhere\", null]\n" +
                "}\n");

        JSONAssert.assertEquals(
            "{\n" +
                "  \"cols\" : [ ],\n" +
                "  \"rows\" : [ ],\n" +
                "  \"rowcount\" : 1,\n" +
                "  \"duration\" : \n" + responseDuration +
                "}", json, true);
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

    @Test
    public void testSqlRequestWithBulkArgs() throws Exception {

        String json = restSQLExecute("{\n" +
                "    \"stmt\": \"insert into locations (id, name, kind) values (?, ?, ?)\",\n" +
                "    \"bulk_args\": [[\"200\", \"Somewhere\", \"planet\"], [\"201\", \"Somewhere else town\", \"city\"]]\n" +
                "}\n");

        JSONAssert.assertEquals(
                "{\n" +
                        "  \"cols\" : [ ],\n" +
                        "  \"results\" : [ {\"rowcount\" : 1 }, {\"rowcount\": 1 } ],\n" +
                        "  \"duration\" : \n" + responseDuration +
                        "}", json, true);
    }

}
