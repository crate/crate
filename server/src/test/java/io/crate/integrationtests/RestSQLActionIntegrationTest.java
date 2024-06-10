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

import static io.crate.testing.Asserts.assertThat;
import static org.assertj.core.api.Assertions.assertThat;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse.BodyHandlers;

import org.junit.Test;

public class RestSQLActionIntegrationTest extends SQLHttpIntegrationTest {

    @Test
    public void test_get() throws Exception {
        var response = get("");
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).startsWith(
            """
            {
              "ok" : true,
              "status" : 200,
              "name" : "node_""");
        response = get("admin");
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).startsWith(
            """
            {
              "ok" : true,
              "status" : 200,
              "name" : "node_""");
    }

    @Test
    public void test_get_wrong_file_path() throws Exception {
        var response = get("/file/path/notexists");
        assertThat(response.statusCode()).isEqualTo(404);
        assertThat(response.body()).isEqualTo("Requested file [file/path/notexists] was not found");
    }

    @Test
    public void test_post_not_allowed_for_incorrect_paths() throws Exception {
        var response = post("/file/path/notexists", (String) null);
        assertThat(response.statusCode()).isEqualTo(403);
        assertThat(response.body()).isEqualTo("POST method is not allowed for [//file/path/notexists]");

        response = post("/file/path/notexists", "{\"stmt\": \"select 1\"}");
        assertThat(response.statusCode()).isEqualTo(403);
        assertThat(response.body()).isEqualTo("POST method is not allowed for [//file/path/notexists]");
    }

    @Test
    public void test_without_body() throws Exception {
        var response = post(null);
        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(response.body()).startsWith(
            "{\"error\":{\"message\":\"SQLParseException[Missing request body]\",\"code\":4000},");
    }

    @Test
    public void test_with_invalid_payload() throws Exception {
        var response = post("{\"foo\": \"bar\"}");
        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(response.body()).startsWith(
            "{\"error\":{\"message\":\"SQLParseException[Failed to parse source" +
            " [{\\\"foo\\\": \\\"bar\\\"}]]\",\"code\":4000},");
    }

    @Test
    public void test_with_args_and_bulk_args() throws Exception {
        var response = post("{\"stmt\": \"INSERT INTO foo (bar) values (?)\", \"args\": [0], \"bulk_args\": [[0], [1]]}");
        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(response.body()).startsWith(
            "{\"error\":{\"message\":\"SQLParseException[" +
            "request body contains args and bulk_args. It's forbidden to provide both]\"");
    }

    @Test
    public void test_empty_bulk_args_with_statement_containing_parameters() throws Exception {
        execute("create table doc.t (id int primary key) with (number_of_replicas = 0)");
        var response = post("{\"stmt\": \"delete from t where id = ?\", \"bulk_args\": []}");
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).startsWith("{\"cols\":[],\"duration\":");
    }

    @Test
    public void test_set_custom_schema() throws Exception {
        execute("create table custom.foo (id string)");
        var response = post("{\"stmt\": \"select * from foo\"}", new String[] { "Default-Schema", "custom"});
        assertThat(response.statusCode()).isEqualTo(200);

        try (var client = HttpClient.newHttpClient()) {
            var request = HttpRequest.newBuilder(uri)
                .POST(BodyPublishers.ofString("{\"stmt\": \"select * from foo\"}"))
                .header("Content-Type", "application/json")
                .build();
            response = client.send(request, BodyHandlers.ofString());
            assertThat(response.statusCode()).isEqualTo(404);
            assertThat(response.body()).contains("RelationUnknown");
        }
    }

    @Test
    public void test_insert_with_mixed_compatible_types() throws Exception {
        execute("create table doc.t1 (x array(float))");
        var resp = post("{\"stmt\": \"insert into doc.t1 (x) values (?)\", \"args\": [[0, 1.0, 1.42]]}");
        assertThat(resp.statusCode()).isEqualTo(200);
        execute("refresh table doc.t1");
        assertThat(execute("select x from doc.t1")).hasRows("[0.0, 1.0, 1.42]");
    }

    @Test
    public void test_execution_error_contains_stack_trace() throws Exception {
        var resp = post("{\"stmt\": \"select 1 / 0\"}");
        assertThat(resp.body()).contains("BinaryScalar.java");
    }

    @Test
    public void test_interval_is_represented_as_text_via_http() throws Exception {
        var resp = post("{\"stmt\": \"select '5 days'::interval as x\"}");
        assertThat(resp.body()).contains("5 days");
    }
}
