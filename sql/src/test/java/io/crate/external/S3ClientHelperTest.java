/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.external;

import com.amazonaws.http.IdleConnectionReaper;
import com.amazonaws.services.s3.AmazonS3;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.net.URL;
import java.util.Date;

import static io.crate.external.S3ClientHelper.ACCESS_KEY_SETTING_NAME;
import static io.crate.external.S3ClientHelper.SECRET_KEY_SETTING_NAME;
import static org.hamcrest.Matchers.startsWith;

public class S3ClientHelperTest extends CrateUnitTest {

    private S3ClientHelper s3ClientHelper;

    @Before
    public void setupS3ClientHelper() {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(ACCESS_KEY_SETTING_NAME, "testS3AccessKey");
        secureSettings.setString(SECRET_KEY_SETTING_NAME, "testS3SecretKey");
        s3ClientHelper = S3ClientHelper.withDefaultRegion(Settings.builder().setSecureSettings(secureSettings).build());
    }

    @After
    public void cleanUpS3() {
        IdleConnectionReaper.shutdown();
    }

    @Test
    public void testClient() throws Exception {
        assertNotNull(s3ClientHelper.client(new URI("s3://baz")));
        assertNotNull(s3ClientHelper.client(new URI("s3://baz/path/to/file")));
        assertNotNull(s3ClientHelper.client(new URI("s3://@baz")));
        assertNotNull(s3ClientHelper.client(new URI("s3://@baz/path/to/file")));
    }

    @Test
    public void testWrongURIEncodingSecretKey() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Invalid URI. Please make sure that given URI is encoded properly.");
        // 'inv/alid' should be 'inv%2Falid'
        // see http://en.wikipedia.org/wiki/UTF-8#Codepage_layout
        s3ClientHelper.client(new URI("s3://foo:inv/alid@baz/path/to/file"));
    }

    @Test
    public void testWrongURIEncodingAccessKey() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Invalid URI. Please make sure that given URI is encoded properly.");
        // 'fo/o' should be 'fo%2Fo'
        // see http://en.wikipedia.org/wiki/UTF-8#Codepage_layout
        s3ClientHelper.client(new URI("s3://fo/o:inv%2Falid@baz"));
    }

    @Test
    public void testWithURICredentials() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("S3 credentials cannot be part of the URI but instead must be stored " +
                                        "inside the Crate keystore");
        s3ClientHelper.client(new URI("s3://user:pass@host/path"));
    }

    @Test
    public void testWithCredentials() throws Exception {
        AmazonS3 s3Client = s3ClientHelper.client(new URI("s3://@host/path"));
        URL url = s3Client.generatePresignedUrl("bucket", "key", new Date(0L));
        assertThat(url.toString(), startsWith("https://bucket.s3-eu-west-1.amazonaws.com/key?"));
    }

    @Test
    public void testWithAccessKeyButNoSecretKeyInKeyStore() throws Exception {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(ACCESS_KEY_SETTING_NAME, "testS3AccessKey");
        S3ClientHelper s3ClientHelper = new S3ClientHelper(Settings.builder().setSecureSettings(secureSettings).build());

        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Both [s3.client.default.access_key] and [s3.client.default.secret_key] S3 " +
                                        "settings for credentials must be stored in the crate.keystore");
        s3ClientHelper.client(new URI("s3://@host/path"));
    }

    @Test
    public void testWithSecretKeyButNoAccessKeyInKeyStore() throws Exception {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(SECRET_KEY_SETTING_NAME, "testS3SecretKey");
        S3ClientHelper s3ClientHelper = new S3ClientHelper(Settings.builder().setSecureSettings(secureSettings).build());

        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Both [s3.client.default.access_key] and [s3.client.default.secret_key] S3 " +
                                        "settings for credentials must be stored in the crate.keystore");
        s3ClientHelper.client(new URI("s3://@host/path"));
    }
}
