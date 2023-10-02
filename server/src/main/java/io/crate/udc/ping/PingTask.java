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

package io.crate.udc.ping;

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

import org.jetbrains.annotations.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import io.crate.common.unit.TimeValue;
import io.crate.monitor.ExtendedNodeInfo;

public class PingTask extends TimerTask {

    private static final TimeValue HTTP_TIMEOUT = new TimeValue(5, TimeUnit.SECONDS);
    private static final Logger LOGGER = LogManager.getLogger(PingTask.class);

    private final ClusterService clusterService;
    private final ExtendedNodeInfo extendedNodeInfo;
    private final String pingUrl;

    private final AtomicLong successCounter = new AtomicLong(0);
    private final AtomicLong failCounter = new AtomicLong(0);

    public PingTask(ClusterService clusterService,
                    ExtendedNodeInfo extendedNodeInfo,
                    String pingUrl) {
        this.clusterService = clusterService;
        this.pingUrl = pingUrl;
        this.extendedNodeInfo = extendedNodeInfo;
    }

    private Map<String, String> getKernelData() {
        return extendedNodeInfo.kernelData();
    }

    private String getClusterId() {
        return clusterService.state().metadata().clusterUUID();
    }

    private Boolean isMasterNode() {
        return clusterService.state().nodes().isLocalNodeElectedMaster();
    }

    private Map<String, Object> getCounters() {
        return Map.of(
            "success", successCounter.get(),
            "failure", failCounter.get()
        );
    }

    @Nullable
    String getHardwareAddress() {
        String macAddress = extendedNodeInfo.networkInfo().primaryInterface().macAddress();
        return macAddress.isEmpty() ? null : macAddress;
    }

    private URL buildPingUrl() throws URISyntaxException, IOException {

        final URI uri = new URI(this.pingUrl);

        // specifying the initialCapacity based on “expected number of elements / load_factor”
        // in this case, the "expected number of elements" = 10 while default load factor = .75
        Map<String, String> queryMap = new HashMap<>(14);
        queryMap.put("cluster_id", getClusterId());
        queryMap.put("kernel", Strings.toString(JsonXContent.builder().map(getKernelData())));
        queryMap.put("master", isMasterNode().toString());
        queryMap.put("ping_count", Strings.toString(JsonXContent.builder().map(getCounters())));
        queryMap.put("hardware_address", getHardwareAddress());
        queryMap.put("num_processors", Integer.toString(Runtime.getRuntime().availableProcessors()));
        queryMap.put("crate_version", Version.CURRENT.externalNumber());
        queryMap.put("java_version", System.getProperty("java.version"));

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Sending data: {}", queryMap);
        }

        List<String> params = new ArrayList<>(queryMap.size());
        for (Map.Entry<String, String> entry : queryMap.entrySet()) {
            String value = entry.getValue();
            if (value != null) {
                params.add(entry.getKey() + '=' + value);
            }
        }
        String query = String.join("&", params);

        return new URI(
            uri.getScheme(), uri.getUserInfo(), uri.getHost(), uri.getPort(),
            uri.getPath(), query, uri.getFragment()
        ).toURL();
    }

    @Override
    public void run() {
        try {
            URL url = buildPingUrl();
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Sending UDC information to {}...", url);
            }
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setConnectTimeout((int) HTTP_TIMEOUT.millis());
            conn.setReadTimeout((int) HTTP_TIMEOUT.millis());

            if (conn.getResponseCode() >= 300) {
                throw new Exception(String.format(Locale.ENGLISH, "%s Responded with Code %d", url.getHost(), conn.getResponseCode()));
            }
            if (LOGGER.isDebugEnabled()) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8));
                String line = reader.readLine();
                while (line != null) {
                    LOGGER.debug(line);
                    line = reader.readLine();
                }
                reader.close();
            } else {
                conn.getInputStream().close();
            }
            successCounter.incrementAndGet();
        } catch (Exception e) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Error sending UDC information", e);
            }
            failCounter.incrementAndGet();
        }
    }
}
