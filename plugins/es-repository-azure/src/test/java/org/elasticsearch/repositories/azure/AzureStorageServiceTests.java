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
        assertThat(client.getEndpoint().toString())
            .isEqualTo("https://myaccount1.blob.my_endpoint_suffix");
    }

    public void testGetSelectedClientDefaultTimeout() {
        final Settings timeoutSettings = Settings.builder()
            .put(buildClientCredSettings())
            .build();
        final AzureStorageService azureStorageService = storageServiceWithSettings(timeoutSettings);
        final CloudBlobClient client = azureStorageService.client().v1();
        assertThat(client.getDefaultRequestOptions().getTimeoutIntervalInMs()).isNull();
    }

    public void testGetSelectedClientNoTimeout() {
        final AzureStorageService azureStorageService = storageServiceWithSettings(buildClientCredSettings());
        final CloudBlobClient client = azureStorageService.client().v1();
        assertThat(client.getDefaultRequestOptions().getTimeoutIntervalInMs()).isNull();
    }

    public void testGetSelectedClientBackoffPolicy() {
        final AzureStorageService azureStorageService = storageServiceWithSettings(buildClientCredSettings());
        final CloudBlobClient client = azureStorageService.client().v1();
        assertThat(client.getDefaultRequestOptions().getRetryPolicyFactory()).isNotNull();
        assertThat(client.getDefaultRequestOptions().getRetryPolicyFactory()).isInstanceOf(RetryExponentialRetry.class);
    }

    public void testGetSelectedClientBackoffPolicyNbRetries() {
        final Settings timeoutSettings = Settings.builder()
            .put(buildClientCredSettings())
            .put("max_retries", 7)
            .build();

        final AzureStorageService azureStorageService = storageServiceWithSettings(timeoutSettings);
        final CloudBlobClient client = azureStorageService.client().v1();
        assertThat(client.getDefaultRequestOptions().getRetryPolicyFactory()).isNotNull();
        assertThat(client.getDefaultRequestOptions().getRetryPolicyFactory()).isInstanceOf(RetryExponentialRetry.class);
    }

    public void testNoProxy() {
        final Settings settings = Settings.builder()
            .put(buildClientCredSettings())
            .build();
        final AzureStorageService mock = storageServiceWithSettings(settings);
        assertThat(mock.storageSettings.getProxy()).isNull();
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

        assertThat(defaultProxy).isNotNull();
        assertThat(defaultProxy.type()).isEqualTo(Proxy.Type.HTTP);
        assertThat(defaultProxy.address()).isEqualTo(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 8080));
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
        assertThat(proxy).isNotNull();
        assertThat(proxy.type()).isEqualTo(Proxy.Type.HTTP);
        assertThat(proxy.address()).isEqualTo(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 8080));
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
        assertThat(proxy).isNotNull();
        assertThat(proxy.type()).isEqualTo(Proxy.Type.SOCKS);
        assertThat(proxy.address()).isEqualTo(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 8080));
    }

    public void testProxyNoHost() {
        final Settings settings = Settings.builder()
            .put(buildClientCredSettings())
            .put("proxy_port", 8080)
            .put("proxy_type", randomFrom("socks", "http"))
            .build();
        assertThatThrownBy(
            () -> storageServiceWithSettings(settings))
            .isInstanceOf(SettingsException.class)
            .hasMessage("Azure Proxy type has been set but proxy host or port is not defined.");
    }

    public void testProxyNoPort() {
        final Settings settings = Settings.builder()
            .put(buildClientCredSettings())
            .put("proxy_host", "127.0.0.1")
            .put("proxy_type", randomFrom("socks", "http"))
            .build();

        assertThatThrownBy(
            () -> storageServiceWithSettings(settings))
            .isInstanceOf(SettingsException.class)
            .hasMessage("Azure Proxy type has been set but proxy host or port is not defined.");
    }

    public void testProxyNoType() {
        final Settings settings = Settings.builder()
            .put(buildClientCredSettings())
            .put("proxy_host", "127.0.0.1")
            .put("proxy_port", 8080)
            .build();

        assertThatThrownBy(
            () -> storageServiceWithSettings(settings))
            .isInstanceOf(SettingsException.class)
            .hasMessage("Azure Proxy port or host have been set but proxy type is not defined.");
    }

    public void testProxyWrongHost() {
        final Settings settings = Settings.builder()
            .put(buildClientCredSettings())
            .put("proxy_type", randomFrom("socks", "http"))
            .put("proxy_host", "thisisnotavalidhostorwehavebeensuperunlucky")
            .put("proxy_port", 8080)
            .build();

        assertThatThrownBy(
            () -> storageServiceWithSettings(settings))
            .isInstanceOf(SettingsException.class)
            .hasMessage("Azure proxy host is unknown.");
    }

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
