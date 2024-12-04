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

package org.elasticsearch.discovery.ec2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.network.NetworkService.CustomNameResolver;

import io.crate.common.SuppressForbidden;
import io.crate.common.io.IOUtils;

/**
 * Resolves certain ec2 related 'meta' hostnames into an actual hostname
 * obtained from ec2 meta-data.
 * <p>
 * Valid config values for {@link Ec2HostnameType}s are -
 * <ul>
 * <li>_ec2_ - maps to privateIpv4</li>
 * <li>_ec2:privateIp_ - maps to privateIpv4</li>
 * <li>_ec2:privateIpv4_</li>
 * <li>_ec2:privateDns_</li>
 * <li>_ec2:publicIp_ - maps to publicIpv4</li>
 * <li>_ec2:publicIpv4_</li>
 * <li>_ec2:publicDns_</li>
 * </ul>
 *
 * @author Paul_Loy (keteracel)
 */
class Ec2NameResolver implements CustomNameResolver {

    private static final Logger LOGGER = LogManager.getLogger(Ec2NameResolver.class);

    /**
     * enum that can be added to over time with more meta-data types (such as ipv6 when this is available)
     *
     * @author Paul_Loy
     */
    private enum Ec2HostnameType {

        PRIVATE_IPv4("ec2:privateIpv4", "local-ipv4"),
        PRIVATE_DNS("ec2:privateDns", "local-hostname"),
        PUBLIC_IPv4("ec2:publicIpv4", "public-ipv4"),
        PUBLIC_DNS("ec2:publicDns", "public-hostname"),

        // some less verbose defaults
        PUBLIC_IP("ec2:publicIp", PUBLIC_IPv4.ec2Name),
        PRIVATE_IP("ec2:privateIp", PRIVATE_IPv4.ec2Name),
        EC2("ec2", PRIVATE_IPv4.ec2Name);

        final String configName;
        final String ec2Name;

        Ec2HostnameType(String configName, String ec2Name) {
            this.configName = configName;
            this.ec2Name = ec2Name;
        }
    }

    /**
     * @param type the ec2 hostname type to discover.
     * @return the appropriate host resolved from ec2 meta-data, or null if it cannot be obtained.
     * @see CustomNameResolver#resolveIfPossible(String)
     */
    @SuppressForbidden(reason = "We call getInputStream in doPrivileged and provide SocketPermission")
    public List<InetAddress> resolve(Ec2HostnameType type) throws IOException {
        InputStream in = null;
        String metadataUrl = AwsEc2ServiceImpl.EC2_METADATA_URL + type.ec2Name;
        try {
            URL url = new URL(metadataUrl);
            LOGGER.debug("obtaining ec2 hostname from ec2 meta-data url {}", url);
            URLConnection urlConnection = url.openConnection();
            urlConnection.setConnectTimeout(2000);
            in = urlConnection.getInputStream();
            try (BufferedReader urlReader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                String metadataResult = urlReader.readLine();
                if (metadataResult == null || metadataResult.length() == 0) {
                    throw new IOException("no gce metadata returned from [" + url + "] for [" + type.configName + "]");
                }
                // only one address: because we explicitly ask for only one via the Ec2HostnameType
                return List.of(InetAddress.getByName(metadataResult));
            }
        } catch (IOException e) {
            throw new IOException("IOException caught when fetching InetAddress from [" + metadataUrl + "]", e);
        } finally {
            IOUtils.closeWhileHandlingException(in);
        }
    }

    @Override
    public List<InetAddress> resolveDefault() {
        return null; // using this, one has to explicitly specify _ec2_ in network setting
    }

    @Override
    public List<InetAddress> resolveIfPossible(String value) throws IOException {
        for (Ec2HostnameType type : Ec2HostnameType.values()) {
            if (type.configName.equals(value)) {
                return resolve(type);
            }
        }
        return null;
    }

}
