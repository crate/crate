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

package io.crate.protocols.ssl;

import org.elasticsearch.test.ESTestCase;
import io.netty.handler.ssl.SslContext;
import org.elasticsearch.common.settings.Settings;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.security.KeyStore;
import java.security.Security;

import static io.crate.protocols.ssl.SslConfigurationTest.getAbsoluteFilePathFromClassPath;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class SslContextProviderTest extends ESTestCase {

    private static File trustStoreFile;
    private static File keyStoreFile;
    private static String defaultKeyStoreType = KeyStore.getDefaultType();

    @BeforeClass
    public static void beforeTests() throws IOException {
        trustStoreFile = getAbsoluteFilePathFromClassPath("truststore.jks");
        keyStoreFile = getAbsoluteFilePathFromClassPath("keystore.jks");
        Security.setProperty("keystore.type", "jks");
    }

    @AfterClass
    public static void resetKeyStoreType() {
        Security.setProperty("keystore.type", defaultKeyStoreType);
    }

    @Test
    public void testClassLoadingWithInvalidConfiguration() {
        // empty ssl configuration which is invalid
        Settings settings = Settings.builder()
            .put(SslConfigSettings.SSL_HTTP_ENABLED.getKey(), true)
            .put(SslConfigSettings.SSL_PSQL_ENABLED.getKey(), true)
            .build();
        expectedException.expect(SslConfigurationException.class);
        expectedException.expectMessage("Failed to build SSL configuration");
        var sslContextProvider = new SslContextProvider(settings);
        sslContextProvider.getSslContext();
    }

    @Test
    public void testClassLoadingWithValidConfiguration() {
        Settings settings = Settings.builder()
            .put(SslConfigSettings.SSL_HTTP_ENABLED.getKey(), true)
            .put(SslConfigSettings.SSL_PSQL_ENABLED.getKey(), true)
            .put(SslConfigSettings.SSL_TRUSTSTORE_FILEPATH.getKey(), trustStoreFile.getAbsolutePath())
            .put(SslConfigSettings.SSL_TRUSTSTORE_PASSWORD.getKey(), "truststorePassword")
            .put(SslConfigSettings.SSL_KEYSTORE_FILEPATH.getKey(), keyStoreFile.getAbsolutePath())
            .put(SslConfigSettings.SSL_KEYSTORE_PASSWORD.getKey(), "keystorePassword")
            .put(SslConfigSettings.SSL_KEYSTORE_KEY_PASSWORD.getKey(), "serverKeyPassword")
            .build();
        var sslContextProvider = new SslContextProvider(settings);
        SslContext sslContext = sslContextProvider.getSslContext();
        assertThat(sslContext, instanceOf(SslContext.class));
        assertThat(sslContext.isServer(), is(true));
        assertThat(sslContext.cipherSuites(), not(empty()));
    }
}
