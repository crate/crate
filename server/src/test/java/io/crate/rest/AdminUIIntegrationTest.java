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

package io.crate.rest;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URISyntaxException;
import java.net.http.HttpResponse;
import java.util.List;

import org.elasticsearch.test.IntegTestCase;
import org.junit.Test;


@IntegTestCase.ClusterScope(scope = IntegTestCase.Scope.SUITE, numDataNodes = 2)
public class AdminUIIntegrationTest extends AdminUIHttpIntegrationTest {

    @Test
    public void testNonBrowserRequestToRoot() throws Exception {
        //request to root
        assertIsJsonInfoResponse(get(""));
    }

    @Test
    public void testBrowserJsonRequestToRoot() throws Exception {
        assertIsJsonInfoResponse(get("/", browserHeader(), new String[] { "Accept", "application/json" }));
    }

    @Test
    public void testLegacyRedirect() throws Exception, URISyntaxException {
        //request to '/admin' is redirected to '/index.html'
        List<String> allRedirectLocations = getAllRedirectLocations("/admin", browserHeader());
        assertThat(allRedirectLocations).contains("/");
    }

    @Test
    public void testPluginURLRedirect() throws Exception, URISyntaxException {
        //request to '/_plugin/crate-admin' is redirected to '/'

        List<String> allRedirectLocations = getAllRedirectLocations("/_plugin/crate-admin", browserHeader());
        assertThat(allRedirectLocations).contains("/");
    }

    @Test
    public void testPluginURLRedirectReturnsIndex() throws Exception, URISyntaxException {
        //request to '/_plugins/crate-admin' is redirected to '/index.html'
        assertIsIndexResponse(browserGet("/_plugin/crate-admin"));
    }

    @Test
    public void testPostForbidden() throws Exception {
        var response = post("/static/");
        //status should be 403 FORBIDDEN
        assertThat(response.statusCode()).isEqualTo(403);
    }

    @Test
    public void testGetHTML() throws Exception {
        assertIsIndexResponse(browserGet("/"));
        assertIsIndexResponse(browserGet("/index.html"));
        assertIsIndexResponse(browserGet("//index.html"));
    }

    @Test
    public void testNotFound() throws Exception {
        var response = browserGet("/static/does/not/exist.html");
        assertThat(response.statusCode()).isEqualTo(404);
    }

    private static void assertIsIndexResponse(HttpResponse<String> response) throws Exception {
        //response body should not be null
        assertThat(response.body()).startsWith("<h1>Crate Admin</h1>");
        assertThat(response.headers().firstValue("Content-Type")).hasValue("text/html");
    }

    private static void assertIsJsonInfoResponse(HttpResponse<String> response) throws Exception {
        //status should be 200 OK
        assertThat(response.statusCode()).isEqualTo(200);

        assertThat(response.body()).isNotNull();

        //check content-type of response is json
        String contentMimeType = response.headers().firstValue("Content-Type").orElse("");
        assertThat(contentMimeType).isEqualTo("application/json");
    }
}
