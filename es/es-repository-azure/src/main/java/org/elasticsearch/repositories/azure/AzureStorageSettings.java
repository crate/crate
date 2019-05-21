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

import com.microsoft.azure.storage.LocationMode;
import com.microsoft.azure.storage.RetryPolicy;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.unit.TimeValue;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public final class AzureStorageSettings {

    // prefix for azure client settings
    private static final String AZURE_CLIENT_PREFIX = "azure.client.";

    /**
     * Azure account name
     */
    static final Setting<SecureString> ACCOUNT_SETTING = SecureSetting.insecureString(AZURE_CLIENT_PREFIX + "account");

    /**
     * Azure key
     */
    static final Setting<SecureString> KEY_SETTING = SecureSetting.insecureString(AZURE_CLIENT_PREFIX + "key");

    /**
     * max_retries: Number of retries in case of Azure errors.
     * Defaults to 3 (RetryPolicy.DEFAULT_CLIENT_RETRY_COUNT).
     */
    static final Setting<Integer> MAX_RETRIES_SETTING = Setting.intSetting(
        AZURE_CLIENT_PREFIX + "max_retries", RetryPolicy.DEFAULT_CLIENT_RETRY_COUNT, Setting.Property.NodeScope);

    /**
     * Azure endpoint suffix. Default to core.windows.net (CloudStorageAccount.DEFAULT_DNS).
     */
    static final Setting<String> ENDPOINT_SUFFIX_SETTING =
        Setting.simpleString(AZURE_CLIENT_PREFIX + "endpoint_suffix", Property.NodeScope);

    static final Setting<TimeValue> TIMEOUT_SETTING = Setting.timeSetting(
        AZURE_CLIENT_PREFIX + "timeout", TimeValue.timeValueMinutes(-1), Property.NodeScope);

    /**
     * The type of the proxy to connect to azure through. Can be direct (no proxy, default), http or socks
     */
    static final Setting<Proxy.Type> PROXY_TYPE_SETTING = new Setting<>(
        AZURE_CLIENT_PREFIX + "proxy.type",
        "direct",
        s -> Proxy.Type.valueOf(s.toUpperCase(Locale.ROOT)),
        Property.NodeScope);

    /**
     * The host name of a proxy to connect to azure through.
     */
    static final Setting<String> PROXY_HOST_SETTING = Setting.simpleString(
        AZURE_CLIENT_PREFIX + "proxy.host", Property.NodeScope);

    /**
     * The port of a proxy to connect to azure through.
     */
    static final Setting<Integer> PROXY_PORT_SETTING = Setting.intSetting(
        AZURE_CLIENT_PREFIX + "proxy.port", 0, 0, 65535, Setting.Property.NodeScope);

    private final String account;
    private final String key;
    private final String endpointSuffix;
    private final TimeValue timeout;
    private final int maxRetries;
    private final Proxy proxy;
    private final LocationMode locationMode;

    // copy-constructor
    private AzureStorageSettings(String account, String key, String endpointSuffix, TimeValue timeout, int maxRetries, Proxy proxy,
                                 LocationMode locationMode) {
        this.account = account;
        this.key = key;
        this.endpointSuffix = endpointSuffix;
        this.timeout = timeout;
        this.maxRetries = maxRetries;
        this.proxy = proxy;
        this.locationMode = locationMode;
    }

    private AzureStorageSettings(String account, String key, String endpointSuffix, TimeValue timeout, int maxRetries,
                                 Proxy.Type proxyType, String proxyHost, Integer proxyPort) {
        this.account = account;
        this.key = key;
        this.endpointSuffix = endpointSuffix;
        this.timeout = timeout;
        this.maxRetries = maxRetries;
        // Register the proxy if we have any
        // Validate proxy settings
        if (proxyType.equals(Proxy.Type.DIRECT) && ((proxyPort != 0) || Strings.hasText(proxyHost))) {
            throw new SettingsException("Azure Proxy port or host have been set but proxy type is not defined.");
        }
        if ((proxyType.equals(Proxy.Type.DIRECT) == false) && ((proxyPort == 0) || Strings.isEmpty(proxyHost))) {
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
        this.locationMode = LocationMode.PRIMARY_ONLY;
    }

    public String getKey() {
        return key;
    }

    public String getAccount() {
        return account;
    }

    public String getEndpointSuffix() {
        return endpointSuffix;
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
        return connectionStringBuilder.toString();
    }

    public LocationMode getLocationMode() {
        return locationMode;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AzureStorageSettings{");
        sb.append("account='").append(account).append('\'');
        sb.append(", key='").append(key).append('\'');
        sb.append(", timeout=").append(timeout);
        sb.append(", endpointSuffix='").append(endpointSuffix).append('\'');
        sb.append(", maxRetries=").append(maxRetries);
        sb.append(", proxy=").append(proxy);
        sb.append(", locationMode='").append(locationMode).append('\'');
        sb.append('}');
        return sb.toString();
    }

    /**
     * Parse and read all settings available under the azure.client
     * @param settings settings to parse
     * @return All the named configurations
     */
    public static Map<String, AzureStorageSettings> load(Settings settings) {
        return Map.of("default", getClientSettings(settings));
    }

    /**
     * Parse settings for a single client.
     */
    private static AzureStorageSettings getClientSettings(Settings settings) {
        try (SecureString account = getConfigValue(settings, ACCOUNT_SETTING);
             SecureString key = getConfigValue(settings, KEY_SETTING)) {
            return new AzureStorageSettings(
                account.toString(),
                key.toString(),
                getConfigValue(settings, ENDPOINT_SUFFIX_SETTING),
                getConfigValue(settings, TIMEOUT_SETTING),
                getConfigValue(settings, MAX_RETRIES_SETTING),
                getConfigValue(settings, PROXY_TYPE_SETTING),
                getConfigValue(settings, PROXY_HOST_SETTING),
                getConfigValue(settings, PROXY_PORT_SETTING));
        }
    }

    private static <T> T getConfigValue(Settings settings, Setting<T> clientSetting) {
        return clientSetting.get(settings);
    }

    static Map<String, AzureStorageSettings> overrideLocationMode(Map<String, AzureStorageSettings> clientsSettings,
                                                                  LocationMode locationMode) {
        final var map = new HashMap<String, AzureStorageSettings>();
        for (final Map.Entry<String, AzureStorageSettings> entry : clientsSettings.entrySet()) {
            final AzureStorageSettings azureSettings = new AzureStorageSettings(entry.getValue().account, entry.getValue().key,
                    entry.getValue().endpointSuffix, entry.getValue().timeout, entry.getValue().maxRetries, entry.getValue().proxy,
                    locationMode);
            map.put(entry.getKey(), azureSettings);
        }
        return Map.copyOf(map);
    }
}
