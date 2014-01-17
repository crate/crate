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

import com.google.common.base.Joiner;
import org.cratedb.ClusterIdService;
import org.cratedb.Version;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.http.HttpServerTransport;
import org.hyperic.sigar.OperatingSystem;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.*;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class PingTask extends TimerTask {

    public static TimeValue HTTP_TIMEOUT = new TimeValue(5, TimeUnit.SECONDS);

    private ESLogger logger = Loggers.getLogger(this.getClass());

    private final ClusterService clusterService;
    private final ClusterIdService clusterIdService;
    private final HttpServerTransport httpServerTransport;
    private final String pingUrl;

    private AtomicLong successCounter = new AtomicLong(0);
    private AtomicLong failCounter = new AtomicLong(0);

    public PingTask(ClusterService clusterService,
                    ClusterIdService clusterIdService,
                    HttpServerTransport httpServerTransport,
                    String pingUrl) {
        this.clusterService = clusterService;
        this.clusterIdService = clusterIdService;
        this.httpServerTransport = httpServerTransport;
        this.pingUrl = pingUrl;
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> getKernelData() {
        return OperatingSystem.getInstance().toMap();
    }

    public String getClusterId() {
        return clusterIdService.clusterId().value().toString();
    }

    public Boolean isMasterNode() {
        return new Boolean(clusterService.state().nodes().localNodeMaster());
    }

    public Map<String, Object> getCounters() {
        return new HashMap<String, Object>() {{
            put("success", successCounter.get());
            put("failure", failCounter.get());
        }};
    }

    public String getHardwareAddress() {
        String hardwareAddress = null;
        TransportAddress transportAddress = httpServerTransport.boundAddress().publishAddress();
        if (transportAddress instanceof InetSocketTransportAddress) {
            InetAddress inetAddress = ((InetSocketTransportAddress) transportAddress).address().getAddress();
            try {
                NetworkInterface networkInterface = NetworkInterface.getByInetAddress(inetAddress);
                if (networkInterface != null) {
                    byte[] hardwareAddressBytes = networkInterface.getHardwareAddress();
                    StringBuilder sb = new StringBuilder(18);
                    for (byte b : hardwareAddressBytes) {
                        if (sb.length() > 0)
                            sb.append(':');
                        sb.append(String.format("%02x", b));
                    }
                    hardwareAddress = sb.toString();
                }

            } catch (SocketException e) {
                e.printStackTrace();
            }

        }
        return hardwareAddress;
    }

    public String getCrateVersion() {
        return Version.CURRENT.number();
    }

    public String getJavaVersion() {
        return System.getProperty("java.version");
    }

    private URL buildPingUrl() throws URISyntaxException, IOException, NoSuchAlgorithmException {

        URI uri = new URI(this.pingUrl);

        Map<String, String> queryMap = new HashMap<>();
        queryMap.put("kernel", XContentFactory.jsonBuilder().map(getKernelData()).string());
        queryMap.put("cluster_id", getClusterId());
        queryMap.put("master", isMasterNode().toString());
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
        logger.info("Sending UDC information...");
        try {
            URL url = buildPingUrl();
            if (logger.isDebugEnabled()) {
                logger.debug("Sending UDC information to {}...", url);
            }
            HttpURLConnection conn = (HttpURLConnection)url.openConnection();
            conn.setConnectTimeout((int)HTTP_TIMEOUT.millis());
            conn.setReadTimeout((int)HTTP_TIMEOUT.millis());

            if (conn.getResponseCode() >= 300) {
                throw new Exception(String.format("%s Responded with Code %d", url.getHost(), conn.getResponseCode()));
            }
            if (logger.isDebugEnabled()) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()));
                String line = reader.readLine();
                while (line != null) {
                    logger.debug(line);
                    line = reader.readLine();
                }
                reader.close();
            } else {
                conn.getInputStream().close();
            }
        } catch (Exception e) {
            if (logger.isDebugEnabled()) {
                logger.debug("Error sending UDC information", e);
            }
            logger.info("Sending UDC information failed.");
            failCounter.incrementAndGet();
        }
        logger.info("done.");
    }
}
