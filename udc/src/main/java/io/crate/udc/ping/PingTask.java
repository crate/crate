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
import io.crate.ClusterIdService;
import io.crate.Version;
import io.crate.monitor.ExtendedNodeInfo;
import io.crate.settings.SharedSettings;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
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
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class PingTask extends TimerTask {

    private static final TimeValue HTTP_TIMEOUT = new TimeValue(5, TimeUnit.SECONDS);
    private static final Logger logger = Loggers.getLogger(PingTask.class);

    private final ClusterService clusterService;
    private final ClusterIdService clusterIdService;
    private final ExtendedNodeInfo extendedNodeInfo;
    private final String pingUrl;
    private final Settings settings;

    private AtomicLong successCounter = new AtomicLong(0);
    private AtomicLong failCounter = new AtomicLong(0);

    public PingTask(ClusterService clusterService,
                    ClusterIdService clusterIdService,
                    ExtendedNodeInfo extendedNodeInfo,
                    String pingUrl,
                    Settings settings) {
        this.clusterService = clusterService;
        this.clusterIdService = clusterIdService;
        this.extendedNodeInfo = extendedNodeInfo;
        this.pingUrl = pingUrl;
        this.settings = settings;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getKernelData() {
        return extendedNodeInfo.osInfo().kernelData();
    }

    @Nullable
    private String getClusterId() {
        // wait until clusterId is available (master has been elected)
        try {
            return clusterIdService.clusterId().get().value().toString();
        } catch (InterruptedException | ExecutionException e) {
            if (logger.isTraceEnabled()) {
                logger.trace("Error getting cluster id", e);
            }
            return null;
        }
    }

    private Boolean isMasterNode() {
        return clusterService.localNode().isMasterNode();
    }

    @VisibleForTesting
    String isEnterprise() {
        return SharedSettings.ENTERPRISE_LICENSE_SETTING.setting().getRaw(settings);
    }

    private Map<String, Object> getCounters() {
        return new HashMap<String, Object>() {{
            put("success", successCounter.get());
            put("failure", failCounter.get());
        }};
    }

    @Nullable
    @VisibleForTesting
    String getHardwareAddress() {
        String macAddress = extendedNodeInfo.networkInfo().primaryInterface().macAddress();
        return (macAddress == null || macAddress.equals("")) ? null : macAddress.toLowerCase(Locale.ENGLISH);
    }

    private String getCrateVersion() {
        return Version.CURRENT.number();
    }

    private String getJavaVersion() {
        return System.getProperty("java.version");
    }

    private URL buildPingUrl() throws URISyntaxException, IOException, NoSuchAlgorithmException {

        URI uri = new URI(this.pingUrl);

        Map<String, String> queryMap = new HashMap<>();
        queryMap.put("cluster_id", getClusterId()); // block until clusterId is available
        queryMap.put("kernel", XContentFactory.jsonBuilder().map(getKernelData()).string());
        queryMap.put("master", isMasterNode().toString());
        queryMap.put("enterprise", isEnterprise());
        queryMap.put("ping_count", XContentFactory.jsonBuilder().map(getCounters()).string());
        queryMap.put("hardware_address", getHardwareAddress());
        queryMap.put("crate_version", getCrateVersion());
        queryMap.put("java_version", getJavaVersion());

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
