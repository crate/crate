/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.repositories.azure;

import static org.elasticsearch.repositories.azure.AzureStorageService.blobNameFromUri;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.test.ESTestCase;

import com.microsoft.azure.storage.RetryExponentialRetry;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.core.Base64;

public class AzureStorageServiceTests extends ESTestCase {

    private AzureStorageService storageServiceWithSettings(Settings settings) {
        AzureStorageService storageService = new AzureStorageService();
        storageService.refreshSettings(AzureStorageSettings.getClientSettings(settings));
        return storageService;
    }

    public void testCreateClientWithEndpointSuffix() {
        final Settings settings = Settings.builder().put(buildClientCredSettings())
            .put("endpoint_suffix", "my_endpoint_suffix").build();
        final AzureStorageService azureStorageService = storageServiceWithSettings(settings);
        final CloudBlobClient client = azureStorageService.client().v1();
        assertThat(client.getEndpoint().toString(),
                   equalTo("https://myaccount1.blob.my_endpoint_suffix"));
    }

    public void testGetSelectedClientDefaultTimeout() {
        final Settings timeoutSettings = Settings.builder()
            .put(buildClientCredSettings())
            .build();
        final AzureStorageService azureStorageService = storageServiceWithSettings(timeoutSettings);
        final CloudBlobClient client = azureStorageService.client().v1();
        assertThat(client.getDefaultRequestOptions().getTimeoutIntervalInMs(), nullValue());
    }

    public void testGetSelectedClientNoTimeout() {
        final AzureStorageService azureStorageService = storageServiceWithSettings(buildClientCredSettings());
        final CloudBlobClient client = azureStorageService.client().v1();
        assertThat(client.getDefaultRequestOptions().getTimeoutIntervalInMs(), is(nullValue()));
    }

    public void testGetSelectedClientBackoffPolicy() {
        final AzureStorageService azureStorageService = storageServiceWithSettings(buildClientCredSettings());
        final CloudBlobClient client = azureStorageService.client().v1();
        assertThat(client.getDefaultRequestOptions().getRetryPolicyFactory(), is(notNullValue()));
        assertThat(client.getDefaultRequestOptions().getRetryPolicyFactory(), instanceOf(RetryExponentialRetry.class));
    }

    public void testGetSelectedClientBackoffPolicyNbRetries() {
        final Settings timeoutSettings = Settings.builder()
            .put(buildClientCredSettings())
            .put("max_retries", 7)
            .build();

        final AzureStorageService azureStorageService = storageServiceWithSettings(timeoutSettings);
        final CloudBlobClient client = azureStorageService.client().v1();
        assertThat(client.getDefaultRequestOptions().getRetryPolicyFactory(), is(notNullValue()));
        assertThat(client.getDefaultRequestOptions().getRetryPolicyFactory(), instanceOf(RetryExponentialRetry.class));
    }

    public void testNoProxy() {
        final Settings settings = Settings.builder()
            .put(buildClientCredSettings())
            .build();
        final AzureStorageService mock = storageServiceWithSettings(settings);
        assertThat(mock.storageSettings.getProxy(), nullValue());
    }

    public void testProxyHttp() throws UnknownHostException {
        final Settings settings = Settings.builder()
            .put(buildClientCredSettings())
            .put("proxy_host", "127.0.0.1")
            .put("proxy_port", 8080)
            .put("proxy_type", "http")
            .build();
        final AzureStorageService mock = storageServiceWithSettings(settings);
        final Proxy defaultProxy = mock.storageSettings.getProxy();

        assertThat(defaultProxy, notNullValue());
        assertThat(defaultProxy.type(), is(Proxy.Type.HTTP));
        assertThat(defaultProxy.address(), is(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 8080)));
    }

    public void testMultipleProxies() throws UnknownHostException {
        final Settings settings = Settings.builder()
            .put(buildClientCredSettings())
            .put("proxy_host", "127.0.0.1")
            .put("proxy_port", 8080)
            .put("proxy_type", "http")
            .build();
        final AzureStorageService mock = storageServiceWithSettings(settings);
        final Proxy proxy = mock.storageSettings.getProxy();
        assertThat(proxy, notNullValue());
        assertThat(proxy.type(), is(Proxy.Type.HTTP));
        assertThat(proxy.address(), is(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 8080)));
    }

    public void testProxySocks() throws UnknownHostException {
        final Settings settings = Settings.builder()
            .put(buildClientCredSettings())
            .put("proxy_host", "127.0.0.1")
            .put("proxy_port", 8080)
            .put("proxy_type", "socks")
            .build();
        final AzureStorageService mock = storageServiceWithSettings(settings);
        final Proxy proxy = mock.storageSettings.getProxy();
        assertThat(proxy, notNullValue());
        assertThat(proxy.type(), is(Proxy.Type.SOCKS));
        assertThat(proxy.address(), is(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 8080)));
    }

    public void testProxyNoHost() {
        final Settings settings = Settings.builder()
            .put(buildClientCredSettings())
            .put("proxy_port", 8080)
            .put("proxy_type", randomFrom("socks", "http"))
            .build();
        final SettingsException e = expectThrows(SettingsException.class, () -> storageServiceWithSettings(settings));
        assertEquals("Azure Proxy type has been set but proxy host or port is not defined.", e.getMessage());
    }

    public void testProxyNoPort() {
        final Settings settings = Settings.builder()
            .put(buildClientCredSettings())
            .put("proxy_host", "127.0.0.1")
            .put("proxy_type", randomFrom("socks", "http"))
            .build();

        final SettingsException e = expectThrows(SettingsException.class, () -> storageServiceWithSettings(settings));
        assertEquals("Azure Proxy type has been set but proxy host or port is not defined.", e.getMessage());
    }

    public void testProxyNoType() {
        final Settings settings = Settings.builder()
            .put(buildClientCredSettings())
            .put("proxy_host", "127.0.0.1")
            .put("proxy_port", 8080)
            .build();

        final SettingsException e = expectThrows(SettingsException.class, () -> storageServiceWithSettings(settings));
        assertEquals("Azure Proxy port or host have been set but proxy type is not defined.", e.getMessage());
    }

    public void testProxyWrongHost() {
        final Settings settings = Settings.builder()
            .put(buildClientCredSettings())
            .put("proxy_type", randomFrom("socks", "http"))
            .put("proxy_host", "thisisnotavalidhostorwehavebeensuperunlucky")
            .put("proxy_port", 8080)
            .build();

        final SettingsException e = expectThrows(SettingsException.class,
                                                 () -> storageServiceWithSettings(settings));
        assertEquals("Azure proxy host is unknown.", e.getMessage());
    }

    public void testBlobNameFromUri() throws URISyntaxException {
        String name = blobNameFromUri(new URI("https://myservice.azure.net/container/path/to/myfile"));
        assertThat(name, is("path/to/myfile"));
        name = blobNameFromUri(new URI("http://myservice.azure.net/container/path/to/myfile"));
        assertThat(name, is("path/to/myfile"));
        name = blobNameFromUri(new URI("http://127.0.0.1/container/path/to/myfile"));
        assertThat(name, is("path/to/myfile"));
        name = blobNameFromUri(new URI("https://127.0.0.1/container/path/to/myfile"));
        assertThat(name, is("path/to/myfile"));
    }

    private static Settings buildClientCredSettings() {
        return Settings.builder()
            .put("account", "myaccount1")
            .put("key", Base64.encode("mykey1".getBytes(StandardCharsets.UTF_8)))
            .build();
    }
}
