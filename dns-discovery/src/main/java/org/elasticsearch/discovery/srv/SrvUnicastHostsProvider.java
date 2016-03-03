/*
 * Copyright (c) 2015 Grant Rodgers
 *
 *     Permission is hereby granted, free of charge, to any person obtaining
 *     a copy of this software and associated documentation files (the "Software"),
 *     to deal in the Software without restriction, including without limitation
 *     the rights to use, copy, modify, merge, publish, distribute, sublicense,
 *     and/or sell copies of the Software, and to permit persons to whom the Software
 *     is furnished to do so, subject to the following conditions:
 *
 *     The above copyright notice and this permission notice shall be included in
 *     all copies or substantial portions of the Software.
 *
 *     THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 *     EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
 *     OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 *     IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
 *     CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
 *     TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE
 *     OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package org.elasticsearch.discovery.srv;

import com.google.common.collect.Lists;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.discovery.zen.ping.unicast.UnicastHostsProvider;
import org.elasticsearch.transport.TransportService;
import org.xbill.DNS.*;

import java.net.UnknownHostException;
import java.util.List;


@Singleton
public class SrvUnicastHostsProvider extends AbstractComponent implements UnicastHostsProvider {

    public static final String DISCOVERY_SRV_QUERY = "discovery.srv.query";
    public static final String DISCOVERY_SRV_RESOLVER = "discovery.srv.resolver";

    private final TransportService transportService;
    private final Version version;
    private final String query;
    protected final Resolver resolver;

    @Inject
    public SrvUnicastHostsProvider(Settings settings, TransportService transportService, Version version) {
        super(settings);
        this.transportService = transportService;
        this.version = version;
        this.query = settings.get(DISCOVERY_SRV_QUERY);
        this.resolver = resolver(settings);
    }

    @Nullable
    private Resolver resolver(Settings settings) {
        String hostname = null;
        int port = -1;
        String resolverAddress = settings.get(DISCOVERY_SRV_RESOLVER);
        if (resolverAddress != null) {
            String[] parts = resolverAddress.split(":");
            if (parts.length > 0) {
                hostname = parts[0];
                if (parts.length > 1) {
                    try {
                        port = Integer.valueOf(parts[1]);
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
        List<DiscoveryNode> discoNodes = Lists.newArrayList();
        if (query == null) {
            logger.error("DNS query must not be null. Please set '{}'", DISCOVERY_SRV_QUERY);
            return discoNodes;
        }
        try {
            Record[] records = lookupRecords();
            logger.trace("Building dynamic unicast discovery nodes...");
            if (records == null || records.length == 0) {
                logger.debug("No nodes found");
            } else {
                discoNodes = parseRecords(records);
            }
        } catch (TextParseException e) {
            logger.error("Unable to parse DNS query '{}'", query);
            logger.error("DNS lookup exception:", e);
        }
        logger.info("Using dynamic discovery nodes {}", discoNodes);
        return discoNodes;
    }

    protected Record[] lookupRecords() throws TextParseException {
        Lookup lookup = new Lookup(query, Type.SRV);
        if (this.resolver != null) {
            lookup.setResolver(this.resolver);
        }
        return lookup.run();
    }

    protected List<DiscoveryNode> parseRecords(Record[] records) {
        List<DiscoveryNode> discoNodes = Lists.newArrayList();
        for (Record record : records) {
            SRVRecord srv = (SRVRecord) record;

            String hostname = srv.getTarget().toString().replaceFirst("\\.$", "");
            int port = srv.getPort();
            String address = hostname + ":" + port;

            try {
                // TODO: FIX ME!
                TransportAddress[] addresses = null; // transportService.addressesFromString(address);
                logger.trace("adding {}, transport_address {}", address, addresses[0]);
                discoNodes.add(new DiscoveryNode("#srv-" + address, addresses[0], version.minimumCompatibilityVersion()));
            } catch (Exception e) {
                logger.warn("failed to add {}, address {}", e, address);
            }
        }
        return discoNodes;
    }
}
