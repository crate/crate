/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.discovery;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.discovery.zen.UnicastHostsProvider;
import org.elasticsearch.transport.TransportService;
import org.xbill.DNS.*;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


@Singleton
public class SrvUnicastHostsProvider extends AbstractComponent implements UnicastHostsProvider {

    public static final Setting<String> DISCOVERY_SRV_QUERY = Setting.simpleString(
        "discovery.srv.query", Setting.Property.NodeScope);
    public static final Setting<String> DISCOVERY_SRV_RESOLVER = Setting.simpleString(
        "discovery.srv.resolver", Setting.Property.NodeScope);

    private final TransportService transportService;
    private final Version version;
    private final String query;
    protected final Resolver resolver;

    @Inject
    public SrvUnicastHostsProvider(Settings settings, TransportService transportService) {
        super(settings);
        this.transportService = transportService;
        this.version = Version.CURRENT;
        this.query = DISCOVERY_SRV_QUERY.get(settings);
        this.resolver = resolver(settings);
    }

    @Nullable
    private Resolver resolver(Settings settings) {
        String hostname = null;
        int port = -1;
        String resolverAddress = DISCOVERY_SRV_RESOLVER.get(settings);
        if (!Strings.isNullOrEmpty(resolverAddress)) {
            String[] parts = resolverAddress.split(":");
            if (parts.length > 0) {
                hostname = parts[0];
                if (parts.length > 1) {
                    try {
                        port = Integer.parseInt(parts[1]);
                    } catch (Exception e) {
                        logger.warn("Resolver port '{}' is not an integer. Using default port 53", parts[1]);
                    }
                }

            }
        }
        try {
            Resolver resolver = new SimpleResolver(hostname);
            if (port > 0) {
                resolver.setPort(port);
            }
            return resolver;
        } catch (UnknownHostException e) {
            logger.warn("Could not create resolver. Using default resolver.", e);
        }
        return null;
    }

    @Override
    public List<DiscoveryNode> buildDynamicNodes() {
        if (query == null) {
            logger.error("DNS query must not be null. Please set '{}'", DISCOVERY_SRV_QUERY);
            return Collections.emptyList();
        }
        try {
            Record[] records = lookupRecords();
            logger.trace("Building dynamic unicast discovery nodes...");
            if (records == null || records.length == 0) {
                logger.debug("No nodes found");
            } else {
                List<DiscoveryNode> discoNodes = parseRecords(records);
                logger.info("Using dynamic discovery nodes {}", discoNodes);
                return discoNodes;
            }
        } catch (TextParseException e) {
            logger.error("Unable to parse DNS query '{}'", query);
            logger.error("DNS lookup exception:", e);
        }
        return Collections.emptyList();
    }

    protected Record[] lookupRecords() throws TextParseException {
        Lookup lookup = new Lookup(query, Type.SRV);
        if (this.resolver != null) {
            lookup.setResolver(this.resolver);
        }
        return lookup.run();
    }

    protected List<DiscoveryNode> parseRecords(Record[] records) {
        List<DiscoveryNode> discoNodes = new ArrayList<>(records.length);
        for (Record record : records) {
            SRVRecord srv = (SRVRecord) record;

            String hostname = srv.getTarget().toString().replaceFirst("\\.$", "");
            int port = srv.getPort();
            String address = hostname + ":" + port;

            try {
                TransportAddress[] addresses = transportService.addressesFromString(address, 1);
                for (TransportAddress transportAddress : addresses) {
                    logger.trace("adding {}, transport_address {}", address, transportAddress);
                    discoNodes.add(new DiscoveryNode(
                        "#srv-" + address, transportAddress, version.minimumCompatibilityVersion()));
                }
            } catch (Exception e) {
                logger.warn("failed to add {}, address {}", e, address);
            }
        }
        return discoNodes;
    }
}
