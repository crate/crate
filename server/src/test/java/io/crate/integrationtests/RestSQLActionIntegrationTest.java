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


import org.apache.http.Header;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.junit.Test;

import java.io.IOException;

import static io.crate.testing.TestingHelpers.printedTable;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.StringStartsWith.startsWith;

public class RestSQLActionIntegrationTest extends SQLHttpIntegrationTest {

    @Test
    public void testWithoutBody() throws IOException {
        CloseableHttpResponse response = post(null);
        assertEquals(400, response.getStatusLine().getStatusCode());
        String bodyAsString = EntityUtils.toString(response.getEntity());
        assertThat(bodyAsString, startsWith("{\"error\":{\"message\":\"" +
                                            "SQLParseException[Missing request body]\"," +
                                            "\"code\":4000},"
        ));
    }

    @Test
    public void testWithInvalidPayload() throws IOException {
        CloseableHttpResponse response = post("{\"foo\": \"bar\"}");
        assertEquals(400, response.getStatusLine().getStatusCode());
        String bodyAsString = EntityUtils.toString(response.getEntity());
        assertThat(bodyAsString, startsWith("{\"error\":{\"message\":\"SQLParseException[Failed to parse source" +
                                            " [{\\\"foo\\\": \\\"bar\\\"}]]\",\"code\":4000},")
        );
    }

    @Test
    public void testWithArgsAndBulkArgs() throws IOException {
        CloseableHttpResponse response
            = post("{\"stmt\": \"INSERT INTO foo (bar) values (?)\", \"args\": [0], \"bulk_args\": [[0], [1]]}");
        assertEquals(400, response.getStatusLine().getStatusCode());
        String bodyAsString = EntityUtils.toString(response.getEntity());
        assertThat(bodyAsString, startsWith("{\"error\":{\"message\":\"SQLParseException[request body contains args and bulk_args. It's forbidden to provide both]\"")
        );
    }

    @Test
    public void testEmptyBulkArgsWithStatementContainingParameters() throws IOException {
        execute("create table doc.t (id int primary key) with (number_of_replicas = 0)");
        CloseableHttpResponse response = post("{\"stmt\": \"delete from t where id = ?\", \"bulk_args\": []}");
        assertThat(response.getStatusLine().getStatusCode(), is(200));
        String bodyAsString = EntityUtils.toString(response.getEntity());
        assertThat(bodyAsString, startsWith("{\"cols\":[],\"duration\":"));
    }

    @Test
    public void testSetCustomSchema() throws IOException {
        execute("create table custom.foo (id string)");
        Header[] headers = new Header[]{
            new BasicHeader("Default-Schema", "custom")
        };
        CloseableHttpResponse response = post("{\"stmt\": \"select * from foo\"}", headers);
        assertThat(response.getStatusLine().getStatusCode(), is(200));

        response = post("{\"stmt\": \"select * from foo\"}");
        assertThat(response.getStatusLine().getStatusCode(), is(404));
        assertThat(EntityUtils.toString(response.getEntity()), containsString("RelationUnknown"));
    }

    @Test
    public void testInsertWithMixedCompatibleTypes() throws IOException {
        execute("create table doc.t1 (x array(float))");
        CloseableHttpResponse resp = post("{\"stmt\": \"insert into doc.t1 (x) values (?)\", \"args\": [[0, 1.0, 1.42]]}");
        assertThat(resp.getStatusLine().getStatusCode(), is(200));
        execute("refresh table doc.t1");
        assertThat(printedTable(execute("select x from doc.t1").rows()),
            is("[0.0, 1.0, 1.42]\n"));
    }

    @Test
    public void testExecutionErrorContainsStackTrace() throws Exception {
        CloseableHttpResponse resp = post("{\"stmt\": \"select 1 / 0\"}");
        String bodyAsString = EntityUtils.toString(resp.getEntity());
        assertThat(bodyAsString, containsString("BinaryScalar.java"));
    }

    @Test
    public void test_interval_is_represented_as_text_via_http() throws Exception{
        var resp = post("{\"stmt\": \"select '5 days'::interval as x\"}");
        String bodyAsString = EntityUtils.toString(resp.getEntity());
        assertThat(bodyAsString, containsString("5 days"));
    }
}
