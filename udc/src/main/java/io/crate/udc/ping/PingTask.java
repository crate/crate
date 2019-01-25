/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.udc.ping;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import io.crate.license.DecryptedLicenseData;
import io.crate.license.LicenseService;
import io.crate.monitor.ExtendedNodeInfo;
import io.crate.settings.SharedSettings;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class PingTask extends TimerTask {

    private static final TimeValue HTTP_TIMEOUT = new TimeValue(5, TimeUnit.SECONDS);
    private static final Logger logger = Loggers.getLogger(PingTask.class);

    private final ClusterService clusterService;
    private final ExtendedNodeInfo extendedNodeInfo;
    private final String pingUrl;
    private final Settings settings;
    private final LicenseService licenseService;

    private AtomicLong successCounter = new AtomicLong(0);
    private AtomicLong failCounter = new AtomicLong(0);

    public PingTask(ClusterService clusterService,
                    ExtendedNodeInfo extendedNodeInfo,
                    String pingUrl,
                    Settings settings,
                    LicenseService licenseService) {
        this.clusterService = clusterService;
        this.pingUrl = pingUrl;
        this.settings = settings;
        this.extendedNodeInfo = extendedNodeInfo;
        this.licenseService = licenseService;
    }

    private Map<String, String> getKernelData() {
        return extendedNodeInfo.osInfo().kernelData();
    }

    private String getClusterId() {
        return clusterService.state().metaData().clusterUUID();
    }

    private Boolean isMasterNode() {
        return clusterService.localNode().isMasterNode();
    }

    @VisibleForTesting
    String isEnterprise() {
        return SharedSettings.ENTERPRISE_LICENSE_SETTING.setting().getRaw(settings);
    }

    private Map<String, Object> getCounters() {
        return ImmutableMap.of(
            "success", successCounter.get(),
            "failure", failCounter.get()
        );
    }

    @Nullable
    @VisibleForTesting
    String getHardwareAddress() {
        String macAddress = extendedNodeInfo.networkInfo().primaryInterface().macAddress();
        return macAddress.equals("") ? null : macAddress;
    }

    private URL buildPingUrl() throws URISyntaxException, IOException {

        final URI uri = new URI(this.pingUrl);

        // specifying the initialCapacity based on “expected number of elements / load_factor”
        // in this case, the "expected number of elements" = 10 while default load factor = .75
        Map<String, String> queryMap = new HashMap<>(14);
        queryMap.put("cluster_id", getClusterId());
        queryMap.put("kernel", Strings.toString(XContentFactory.jsonBuilder().map(getKernelData())));
        queryMap.put("master", isMasterNode().toString());
        queryMap.put("enterprise", isEnterprise());
        queryMap.put("ping_count", Strings.toString(XContentFactory.jsonBuilder().map(getCounters())));
        queryMap.put("hardware_address", getHardwareAddress());
        queryMap.put("crate_version", Version.CURRENT.externalNumber());
        queryMap.put("java_version", System.getProperty("java.version"));

        DecryptedLicenseData license = licenseService.currentLicense();
        if (license != null) {
            queryMap.put("license_expiry_date", String.valueOf(license.expiryDateInMs()));
            queryMap.put("license_issued_to", license.issuedTo());
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Sending data: {}", queryMap);
        }

        final Joiner joiner = Joiner.on('=');
        List<String> params = new ArrayList<>(queryMap.size());
        for (Map.Entry<String, String> entry : queryMap.entrySet()) {
            if (entry.getValue() != null) {
                params.add(joiner.join(entry.getKey(), entry.getValue()));
            }
        }
        String query = Joiner.on('&').join(params);

        return new URI(
            uri.getScheme(), uri.getUserInfo(), uri.getHost(), uri.getPort(),
            uri.getPath(), query, uri.getFragment()
        ).toURL();
    }

    @Override
    public void run() {
        try {
            URL url = buildPingUrl();
            if (logger.isDebugEnabled()) {
                logger.debug("Sending UDC information to {}...", url);
            }
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setConnectTimeout((int) HTTP_TIMEOUT.millis());
            conn.setReadTimeout((int) HTTP_TIMEOUT.millis());

            if (conn.getResponseCode() >= 300) {
                throw new Exception(String.format(Locale.ENGLISH, "%s Responded with Code %d", url.getHost(), conn.getResponseCode()));
            }
            if (logger.isDebugEnabled()) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8));
                String line = reader.readLine();
                while (line != null) {
                    logger.debug(line);
                    line = reader.readLine();
                }
                reader.close();
            } else {
                conn.getInputStream().close();
            }
            successCounter.incrementAndGet();
        } catch (Exception e) {
            if (logger.isDebugEnabled()) {
                logger.debug("Error sending UDC information", e);
            }
            failCounter.incrementAndGet();
        }
    }
}
