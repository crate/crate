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

package io.crate.protocols.http;

import static io.crate.protocols.ssl.SslContextProviderTest.getAbsoluteFilePathFromClassPath;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.IOException;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.IntegTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import io.crate.integrationtests.SQLHttpIntegrationTest;
import io.crate.protocols.ssl.SslSettings;
import io.crate.testing.UseJdbc;


@UseJdbc(value = 0)
@IntegTestCase.ClusterScope(numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class CrateHttpsTransportIntegrationTest extends SQLHttpIntegrationTest {

    private static File trustStoreFile;
    private static File keyStoreFile;

    public CrateHttpsTransportIntegrationTest() {
        super(true);
    }

    @BeforeClass
    public static void beforeIntegrationTest() throws IOException {
        keyStoreFile = getAbsoluteFilePathFromClassPath("keystore.pcks12");
        trustStoreFile = getAbsoluteFilePathFromClassPath("truststore.pcks12");
        System.setProperty("javax.net.ssl.trustStore", getAbsoluteFilePathFromClassPath("truststore.jks").getAbsolutePath());
        System.setProperty("javax.net.ssl.trustStorePassword", "keystorePassword");
    }

    @AfterClass
    public static void afterIntegrationTest() {
        System.clearProperty("javax.net.ssl.trustStore");
        System.clearProperty("javax.net.ssl.trustStorePassword");
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(SslSettings.SSL_HTTP_ENABLED.getKey(), true)
            .put(SslSettings.SSL_KEYSTORE_FILEPATH.getKey(), keyStoreFile.getAbsolutePath())
            .put(SslSettings.SSL_KEYSTORE_PASSWORD.getKey(), "keystorePassword")
            .put(SslSettings.SSL_KEYSTORE_KEY_PASSWORD.getKey(), "keystorePassword")
            .put(SslSettings.SSL_TRUSTSTORE_FILEPATH.getKey(), trustStoreFile.getAbsolutePath())
            .put(SslSettings.SSL_TRUSTSTORE_PASSWORD.getKey(), "keystorePassword")
            .build();
    }

    @Test
    public void testCheckEncryptedConnection() throws Throwable {
        CloseableHttpResponse response = post("{\"stmt\": \"select 'sslWorks'\"}");
        assertThat(response).isNotNull();
        assertEquals(200, response.getStatusLine().getStatusCode());
        String result = EntityUtils.toString(response.getEntity());
        assertThat(result, containsString("\"rowcount\":1"));
        assertThat(result, containsString("sslWorks"));
    }

    @Test
    public void testBlobLayer() throws IOException {
        try {
            execute("create blob table test");
            String blob = "abcdefghijklmnopqrstuvwxyz".repeat(1024 * 600);
            String blobUrl = upload("test", blob);
            assertThat(blobUrl, not(isEmptyOrNullString()));
            HttpGet httpGet = new HttpGet(blobUrl);
            CloseableHttpResponse response = httpClient.execute(httpGet);
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertThat(response.getEntity().getContentLength()).isEqualTo((long) blob.length());
        } finally {
            execute("drop table if exists test");
        }
    }
}
