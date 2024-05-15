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
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Locale;

import org.apache.http.Header;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.entity.ContentType;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.test.IntegTestCase;
import org.hamcrest.Matchers;
import org.junit.Test;


@IntegTestCase.ClusterScope(scope = IntegTestCase.Scope.SUITE, numDataNodes = 2)
public class AdminUIIntegrationTest extends AdminUIHttpIntegrationTest {

    private URI adminURI() throws URISyntaxException {
        return new URI(String.format(Locale.ENGLISH, "http://%s:%d/", address.getHostName(), address.getPort()));
    }

    @Test
    public void testNonBrowserRequestToRoot() throws IOException {
        //request to root
        assertIsJsonInfoResponse(get(""));
    }

    @Test
    public void testBrowserJsonRequestToRoot() throws IOException {
        Header[] headers = {
            browserHeader(),
            new BasicHeader("Accept", "application/json")
        };
        assertIsJsonInfoResponse(get("/", headers));
    }

    @Test
    public void testLegacyRedirect() throws IOException, URISyntaxException {
        //request to '/admin' is redirected to '/index.html'
        Header[] headers = {
            browserHeader()
        };

        List<URI> allRedirectLocations = getAllRedirectLocations("/admin", headers);

        // all redirect locations should not be null
        assertThat(allRedirectLocations).isNotNull();
        // all redirect locations should contain the crateAdminUI URI
        assertThat(allRedirectLocations.contains(adminURI())).isTrue();
    }

    @Test
    public void testPluginURLRedirect() throws IOException, URISyntaxException {
        //request to '/_plugin/crate-admin' is redirected to '/'
        Header[] headers = {
            browserHeader()
        };

        List<URI> allRedirectLocations = getAllRedirectLocations("/_plugin/crate-admin", headers);

        // all redirect locations should contain the crateAdminUI URI
        assertThat(allRedirectLocations.contains(adminURI())).isTrue();
    }

    @Test
    public void testPluginURLRedirectReturnsIndex() throws IOException, URISyntaxException {
        //request to '/_plugins/crate-admin' is redirected to '/index.html'
        assertIsIndexResponse(browserGet("/_plugin/crate-admin"));
    }

    @Test
    public void testPostForbidden() throws IOException {
        CloseableHttpResponse response = post("/static/");
        //status should be 403 FORBIDDEN
        assertThat(response.getStatusLine().getStatusCode()).isEqualTo(403);
    }

    @Test
    public void testGetHTML() throws IOException {
        assertIsIndexResponse(browserGet("/"));
        assertIsIndexResponse(browserGet("/index.html"));
        assertIsIndexResponse(browserGet("//index.html"));
    }

    @Test
    public void testNotFound() throws Exception {
        CloseableHttpResponse response = browserGet("/static/does/not/exist.html");
        assertThat(response.getStatusLine().getStatusCode()).isEqualTo(404);
    }

    private static void assertIsIndexResponse(CloseableHttpResponse response) throws IOException {
        //response body should not be null
        String bodyAsString = EntityUtils.toString(response.getEntity());
        assertThat(bodyAsString, Matchers.startsWith("<h1>Crate Admin</h1>"));
        assertThat(response.getHeaders("Content-Type")[0].getValue()).isEqualTo("text/html");
    }

    private static void assertIsJsonInfoResponse(CloseableHttpResponse response) throws IOException {
        //status should be 200 OK
        assertThat(response.getStatusLine().getStatusCode()).isEqualTo(200);

        //response body should not be null
        String bodyAsString = EntityUtils.toString(response.getEntity());
        assertThat(bodyAsString).isNotNull();

        //check content-type of response is json
        String contentMimeType = ContentType.getOrDefault(response.getEntity()).getMimeType();
        assertThat(contentMimeType).isEqualTo(ContentType.APPLICATION_JSON.getMimeType());
    }
}
