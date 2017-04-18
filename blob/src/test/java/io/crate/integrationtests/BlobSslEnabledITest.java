/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.integrationtests;

import io.crate.plugin.CrateCorePlugin;
import io.crate.rest.CrateRestHandlerWrapper;
import io.crate.testing.SslDummyPlugin;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.common.network.NetworkModule.HTTP_TYPE_KEY;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 2)
public class BlobSslEnabledITest extends BlobHttpIntegrationTest {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(CrateRestHandlerWrapper.ES_API_ENABLED_SETTING.getKey(), true)
            .put(HTTP_TYPE_KEY, "crate_ssl")
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(CrateCorePlugin.class, SslDummyPlugin.class);
    }

    private String uploadSmallBlob() throws IOException {
        String digest = "c520e6109835c876fd98636efec43dd61634b7d3";
        CloseableHttpResponse response = put(blobUri(digest), StringUtils.repeat("a", 1500));
        assertThat(response.getStatusLine().getStatusCode(), is(201));
        return digest;
    }

    @Test
    public void testRedirectContainsHttpsScheme() throws Exception {
        String blobUri = blobUri(uploadSmallBlob());
        CloseableHttpClient client = HttpClients.custom().disableRedirectHandling().build();
        List<String> redirectLocations = getRedirectLocations(client, blobUri, dataNode1);
        if (redirectLocations.isEmpty()) {
            redirectLocations = getRedirectLocations(client, blobUri, dataNode2);
        }
        String uri = redirectLocations.iterator().next();
        assertThat(uri, startsWith("https"));
    }

    @Test
    public void testGetBlob() throws Exception {
        // this test verifies that the non-zero-copy code path in the HttpBlobHandler works
        String digest = uploadSmallBlob();
        String blobUri = blobUri(digest);

        // can't follow redirects because ssl isn't really enabled
        // -> figure out the node that really has the blob

        CloseableHttpClient client = HttpClients.custom().disableRedirectHandling().build();
        List<String> redirectLocations = getRedirectLocations(client, blobUri, randomNode);
        String redirectUri;
        if (redirectLocations.isEmpty()) {
            redirectUri = String.format(Locale.ENGLISH,
                "http://%s:%s/_blobs/%s", randomNode.getHostName(), randomNode.getPort(), blobUri);
        } else {
            redirectUri = redirectLocations.get(0).replace("https", "http");
        }

        HttpGet httpGet = new HttpGet(redirectUri);

        CloseableHttpResponse response = client.execute(httpGet);
        assertEquals(1500, response.getEntity().getContentLength());
    }
}
