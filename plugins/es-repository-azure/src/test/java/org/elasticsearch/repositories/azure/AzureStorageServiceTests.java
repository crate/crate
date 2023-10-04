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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.elasticsearch.repositories.azure.AzureStorageService.blobNameFromUri;

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
import org.junit.Test;

import com.microsoft.azure.storage.RetryExponentialRetry;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.core.Base64;

public class AzureStorageServiceTests extends ESTestCase {

    private AzureStorageService storageServiceWithSettings(Settings settings) {
        return new AzureStorageService(AzureStorageSettings.getClientSettings(settings));
    }

    @Test
    public void test_cannot_set_endpoint_and_endpoint_suffix() {
        final Settings settings = Settings.builder().put(buildClientCredSettings())
            .put("endpoint", "my_endpoint")
            .put("endpoint_suffix", "my_endpoint_suffix").build();
        assertThatThrownBy(() -> storageServiceWithSettings(settings))
            .isExactlyInstanceOf(SettingsException.class)
            .hasMessage("Cannot specify both endpoint and endpoint_suffix parameters");
    }

    @Test
    public void test_cannot_set_secondary_endpoint_without_endpoint() {
        final Settings settings = Settings.builder().put(buildClientCredSettings())
            .put("secondary_endpoint", "my_secondary_endpoint").build();
        assertThatThrownBy(() -> storageServiceWithSettings(settings))
            .isExactlyInstanceOf(SettingsException.class)
            .hasMessage("Cannot specify secondary_endpoint without specifying endpoint");
    }

    @Test
    public void testCreateClientWithEndpointSuffix() {
        final Settings settings = Settings.builder().put(buildClientCredSettings())
            .put("endpoint_suffix", "my_endpoint_suffix").build();
        final AzureStorageService azureStorageService = storageServiceWithSettings(settings);
        final CloudBlobClient client = azureStorageService.client().cloudBlobClient();
        assertThat(client.getEndpoint()).hasToString("https://myaccount1.blob.my_endpoint_suffix");
    }

    @Test
    public void testCreateClientWithEndpoint() {
        final Settings settings = Settings.builder().put(buildClientCredSettings())
            .put("endpoint", "https://storage1.privatelink.blob.core.windows.net").build();
        final AzureStorageService azureStorageService = storageServiceWithSettings(settings);
        final CloudBlobClient client = azureStorageService.client().cloudBlobClient();
        assertThat(client.getEndpoint()).hasToString("https://storage1.privatelink.blob.core.windows.net");
    }

    @Test
    public void testCreateClientWithSecondaryEndpoint() {
        final Settings settings = Settings.builder().put(buildClientCredSettings())
            .put("location_mode", "SECONDARY_ONLY")
            .put("endpoint", "https://storage1.privatelink.blob.core.windows.net")
            .put("secondary_endpoint", "https://storage2.privatelink.blob.core.windows.net").build();
        final AzureStorageService azureStorageService = storageServiceWithSettings(settings);
        final CloudBlobClient client = azureStorageService.client().cloudBlobClient();
        assertThat(client.getEndpoint()).hasToString("https://storage1.privatelink.blob.core.windows.net");
        assertThat(client.getStorageUri().getPrimaryUri())
            .hasToString("https://storage1.privatelink.blob.core.windows.net");
        assertThat(client.getStorageUri().getSecondaryUri())
            .hasToString("https://storage2.privatelink.blob.core.windows.net");
    }

    @Test
    public void testGetSelectedClientDefaultTimeout() {
        final Settings timeoutSettings = Settings.builder()
            .put(buildClientCredSettings())
            .build();
        final AzureStorageService azureStorageService = storageServiceWithSettings(timeoutSettings);
        final CloudBlobClient client = azureStorageService.client().cloudBlobClient();
        assertThat(client.getDefaultRequestOptions().getTimeoutIntervalInMs()).isNull();
    }

    @Test
    public void testGetSelectedClientNoTimeout() {
        final AzureStorageService azureStorageService = storageServiceWithSettings(buildClientCredSettings());
        final CloudBlobClient client = azureStorageService.client().cloudBlobClient();
        assertThat(client.getDefaultRequestOptions().getTimeoutIntervalInMs()).isNull();
    }

    @Test
    public void testGetSelectedClientBackoffPolicy() {
        final AzureStorageService azureStorageService = storageServiceWithSettings(buildClientCredSettings());
        final CloudBlobClient client = azureStorageService.client().cloudBlobClient();
        assertThat(client.getDefaultRequestOptions().getRetryPolicyFactory()).isNotNull();
        assertThat(client.getDefaultRequestOptions().getRetryPolicyFactory()).isExactlyInstanceOf(RetryExponentialRetry.class);
    }

    @Test
    public void testGetSelectedClientBackoffPolicyNbRetries() {
        final Settings timeoutSettings = Settings.builder()
            .put(buildClientCredSettings())
            .put("max_retries", 7)
            .build();

        final AzureStorageService azureStorageService = storageServiceWithSettings(timeoutSettings);
        final CloudBlobClient client = azureStorageService.client().cloudBlobClient();
        assertThat(client.getDefaultRequestOptions().getRetryPolicyFactory()).isNotNull();
        assertThat(client.getDefaultRequestOptions().getRetryPolicyFactory()).isExactlyInstanceOf(RetryExponentialRetry.class);
    }

    @Test
    public void testNoProxy() {
        final Settings settings = Settings.builder()
            .put(buildClientCredSettings())
            .build();
        final AzureStorageService mock = storageServiceWithSettings(settings);
        assertThat(mock.storageSettings.getProxy()).isNull();
    }

    @Test
    public void testProxyHttp() throws UnknownHostException {
        final Settings settings = Settings.builder()
            .put(buildClientCredSettings())
            .put("proxy_host", "127.0.0.1")
            .put("proxy_port", 8080)
            .put("proxy_type", "http")
            .build();
        final AzureStorageService mock = storageServiceWithSettings(settings);
        final Proxy defaultProxy = mock.storageSettings.getProxy();

        assertThat(defaultProxy).isNotNull();
        assertThat(defaultProxy.type()).isEqualTo(Proxy.Type.HTTP);
        assertThat(defaultProxy.address()).isEqualTo(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 8080));
    }

    @Test
    public void testMultipleProxies() throws UnknownHostException {
        final Settings settings = Settings.builder()
            .put(buildClientCredSettings())
            .put("proxy_host", "127.0.0.1")
            .put("proxy_port", 8080)
            .put("proxy_type", "http")
            .build();
        final AzureStorageService mock = storageServiceWithSettings(settings);
        final Proxy proxy = mock.storageSettings.getProxy();
        assertThat(proxy).isNotNull();
        assertThat(proxy.type()).isEqualTo(Proxy.Type.HTTP);
        assertThat(proxy.address()).isEqualTo(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 8080));
    }

    @Test
    public void testProxySocks() throws UnknownHostException {
        final Settings settings = Settings.builder()
            .put(buildClientCredSettings())
            .put("proxy_host", "127.0.0.1")
            .put("proxy_port", 8080)
            .put("proxy_type", "socks")
            .build();
        final AzureStorageService mock = storageServiceWithSettings(settings);
        final Proxy proxy = mock.storageSettings.getProxy();
        assertThat(proxy).isNotNull();
        assertThat(proxy.type()).isEqualTo(Proxy.Type.SOCKS);
        assertThat(proxy.address()).isEqualTo(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 8080));
    }

    @Test
    public void testProxyNoHost() {
        final Settings settings = Settings.builder()
            .put(buildClientCredSettings())
            .put("proxy_port", 8080)
            .put("proxy_type", randomFrom("socks", "http"))
            .build();
        assertThatThrownBy(
            () -> storageServiceWithSettings(settings))
            .isExactlyInstanceOf(SettingsException.class)
            .hasMessage("Azure Proxy type has been set but proxy host or port is not defined.");
    }

    @Test
    public void testProxyNoPort() {
        final Settings settings = Settings.builder()
            .put(buildClientCredSettings())
            .put("proxy_host", "127.0.0.1")
            .put("proxy_type", randomFrom("socks", "http"))
            .build();

        assertThatThrownBy(
            () -> storageServiceWithSettings(settings))
            .isExactlyInstanceOf(SettingsException.class)
            .hasMessage("Azure Proxy type has been set but proxy host or port is not defined.");
    }

    @Test
    public void testProxyNoType() {
        final Settings settings = Settings.builder()
            .put(buildClientCredSettings())
            .put("proxy_host", "127.0.0.1")
            .put("proxy_port", 8080)
            .build();

        assertThatThrownBy(
            () -> storageServiceWithSettings(settings))
            .isExactlyInstanceOf(SettingsException.class)
            .hasMessage("Azure Proxy port or host have been set but proxy type is not defined.");
    }

    @Test
    public void testProxyWrongHost() {
        final Settings settings = Settings.builder()
            .put(buildClientCredSettings())
            .put("proxy_type", randomFrom("socks", "http"))
            .put("proxy_host", "thisisnotavalidhostorwehavebeensuperunlucky")
            .put("proxy_port", 8080)
            .build();

        assertThatThrownBy(
            () -> storageServiceWithSettings(settings))
            .isExactlyInstanceOf(SettingsException.class)
            .hasMessage("Azure proxy host is unknown.");
    }

    @Test
    public void testBlobNameFromUri() throws URISyntaxException {
        String name = blobNameFromUri(new URI("https://myservice.azure.net/container/path/to/myfile"));
        assertThat(name).isEqualTo("path/to/myfile");
        name = blobNameFromUri(new URI("http://myservice.azure.net/container/path/to/myfile"));
        assertThat(name).isEqualTo("path/to/myfile");
        name = blobNameFromUri(new URI("http://127.0.0.1/container/path/to/myfile"));
        assertThat(name).isEqualTo("path/to/myfile");
        name = blobNameFromUri(new URI("https://127.0.0.1/container/path/to/myfile"));
        assertThat(name).isEqualTo("path/to/myfile");
    }

    private static Settings buildClientCredSettings() {
        return Settings.builder()
            .put("account", "myaccount1")
            .put("key", Base64.encode("mykey1".getBytes(StandardCharsets.UTF_8)))
            .build();
    }
}
