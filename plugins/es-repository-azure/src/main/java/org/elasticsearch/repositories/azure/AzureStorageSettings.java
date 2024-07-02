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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.UnknownHostException;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;

import com.microsoft.azure.storage.LocationMode;

import org.jetbrains.annotations.VisibleForTesting;
import io.crate.common.unit.TimeValue;

public final class AzureStorageSettings {

    private final String account;
    private final String key;
    private final String endpoint;
    private final String secondaryEndpoint;
    private final String endpointSuffix;
    private final TimeValue timeout;
    private final int maxRetries;
    private final Proxy proxy;
    private final LocationMode locationMode;

    @VisibleForTesting
    AzureStorageSettings(String account,
                         String key,
                         String endpoint,
                         String secondaryEndpoint,
                         String endpointSuffix,
                         TimeValue timeout,
                         int maxRetries,
                         Proxy proxy,
                         LocationMode locationMode) {
        this.account = account;
        this.key = key;
        this.endpoint = endpoint;
        this.secondaryEndpoint = secondaryEndpoint;
        this.endpointSuffix = endpointSuffix;
        this.timeout = timeout;
        this.maxRetries = maxRetries;
        this.proxy = proxy;
        this.locationMode = locationMode;
    }

    private AzureStorageSettings(String account,
                                 String key,
                                 LocationMode locationMode,
                                 String endpoint,
                                 String secondaryEndpoint,
                                 String endpointSuffix,
                                 TimeValue timeout,
                                 int maxRetries,
                                 Proxy.Type proxyType,
                                 String proxyHost,
                                 Integer proxyPort) {

        final boolean hasEndpointSuffix = Strings.hasText(endpointSuffix);
        final boolean hasEndpoint = Strings.hasText(endpoint);
        final boolean hasSecondaryEndpoint = Strings.hasText(secondaryEndpoint);

        if (hasEndpoint && hasEndpointSuffix) {
            throw new SettingsException("Cannot specify both endpoint and endpoint_suffix parameters");
        }
        if (hasSecondaryEndpoint && hasEndpoint == false) {
            throw new SettingsException("Cannot specify secondary_endpoint without specifying endpoint");
        }

        this.account = account;
        this.key = key;
        this.endpoint = endpoint;
        this.secondaryEndpoint = secondaryEndpoint;
        this.endpointSuffix = endpointSuffix;
        this.timeout = timeout;
        this.maxRetries = maxRetries;
        // Register the proxy if we have any
        // Validate proxy settings
        if (proxyType.equals(Proxy.Type.DIRECT) && ((proxyPort != 0) || Strings.hasText(proxyHost))) {
            throw new SettingsException("Azure Proxy port or host have been set but proxy type is not defined.");
        }
        if ((proxyType.equals(Proxy.Type.DIRECT) == false) && ((proxyPort == 0) || Strings.isNullOrEmpty(proxyHost))) {
            throw new SettingsException("Azure Proxy type has been set but proxy host or port is not defined.");
        }

        if (proxyType.equals(Proxy.Type.DIRECT)) {
            proxy = null;
        } else {
            try {
                proxy = new Proxy(proxyType, new InetSocketAddress(InetAddress.getByName(proxyHost), proxyPort));
            } catch (final UnknownHostException e) {
                throw new SettingsException("Azure proxy host is unknown.", e);
            }
        }
        this.locationMode = locationMode;
    }

    public TimeValue getTimeout() {
        return timeout;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public Proxy getProxy() {
        return proxy;
    }

    public LocationMode getLocationMode() {
        return locationMode;
    }

    public String buildConnectionString() {
        final StringBuilder connectionStringBuilder = new StringBuilder();
        connectionStringBuilder.append("DefaultEndpointsProtocol=https")
                .append(";AccountName=")
                .append(account)
                .append(";AccountKey=")
                .append(key);

        if (Strings.hasText(endpointSuffix)) {
            connectionStringBuilder.append(";EndpointSuffix=").append(endpointSuffix);
        }
        if (Strings.hasText(endpoint)) {
            connectionStringBuilder.append(";BlobEndpoint=").append(endpoint);
        }
        if (Strings.hasText(secondaryEndpoint)) {
            connectionStringBuilder.append(";BlobSecondaryEndpoint=").append(secondaryEndpoint);
        }
        return connectionStringBuilder.toString();
    }

    static AzureStorageSettings getClientSettings(Settings settings) {
        try (SecureString account = getConfigValue(settings, AzureRepository.Repository.ACCOUNT_SETTING);
             SecureString key = getConfigValue(settings, AzureRepository.Repository.KEY_SETTING)) {
            return new AzureStorageSettings(
                account.toString(),
                key.toString(),
                getConfigValue(settings, AzureRepository.Repository.LOCATION_MODE_SETTING),
                getConfigValue(settings, AzureRepository.Repository.ENDPOINT_SETTING),
                getConfigValue(settings, AzureRepository.Repository.SECONDARY_ENDPOINT_SETTING),
                getConfigValue(settings, AzureRepository.Repository.ENDPOINT_SUFFIX_SETTING),
                getConfigValue(settings, AzureRepository.Repository.TIMEOUT_SETTING),
                getConfigValue(settings, AzureRepository.Repository.MAX_RETRIES_SETTING),
                getConfigValue(settings, AzureRepository.Repository.PROXY_TYPE_SETTING),
                getConfigValue(settings, AzureRepository.Repository.PROXY_HOST_SETTING),
                getConfigValue(settings, AzureRepository.Repository.PROXY_PORT_SETTING));
        }
    }

    private static <T> T getConfigValue(Settings settings, Setting<T> clientSetting) {
        return clientSetting.get(settings);
    }

    static AzureStorageSettings copy(AzureStorageSettings settings) {
        return new AzureStorageSettings(
            settings.account,
            settings.key,
            settings.endpoint,
            settings.secondaryEndpoint,
            settings.endpointSuffix,
            settings.timeout,
            settings.maxRetries,
            settings.proxy,
            settings.locationMode);
    }

    @Override
    public String toString() {
        return "AzureStorageSettings{" + "account='" + account + '\'' +
               ", key='" + key + '\'' +
               ", timeout=" + timeout +
               ", endpoint='" + endpoint + '\'' +
               ", secondaryEndpoint='" + secondaryEndpoint + '\'' +
               ", endpointSuffix='" + endpointSuffix + '\'' +
               ", maxRetries=" + maxRetries +
               ", proxy=" + proxy +
               ", locationMode='" + locationMode + '\'' +
               '}';
    }
}
