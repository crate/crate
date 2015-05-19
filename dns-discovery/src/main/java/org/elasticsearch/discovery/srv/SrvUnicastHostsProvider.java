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
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.discovery.zen.ping.unicast.UnicastHostsProvider;
import org.elasticsearch.transport.TransportService;
import org.xbill.DNS.*;

import java.util.List;


@Singleton
public class SrvUnicastHostsProvider extends AbstractComponent implements UnicastHostsProvider {

    private final TransportService transportService;
    private final Version version;
    private final String query;

    @Inject
    public SrvUnicastHostsProvider(Settings settings, TransportService transportService, Version version) {
        super(settings);
        this.transportService = transportService;
        this.version = version;

        this.query = settings.get("discovery.srv.query");
    }

    @Override
    public List<DiscoveryNode> buildDynamicNodes() {
        List<DiscoveryNode> discoNodes = Lists.newArrayList();

        if (query == null) {
            logger.error("DNS query must not be null.");
            return discoNodes;
        }
        try {
            Record[] records = new Lookup(query, Type.SRV).run();

            logger.trace("building dynamic unicast discovery nodes...");
            if (records == null) {
                logger.debug("No nodes found");
            } else {
                for (Record record : records) {
                    SRVRecord srv = (SRVRecord) record;

                    String hostname = srv.getTarget().toString().replaceFirst("\\.$", "");
                    int port = srv.getPort();
                    String address = hostname + ":" + port;

                    try {
                        TransportAddress[] addresses = transportService.addressesFromString(address);
                        logger.trace("adding {}, transport_address {}", address, addresses[0]);
                        discoNodes.add(new DiscoveryNode("#srv-" + address, addresses[0], version.minimumCompatibilityVersion()));
                    } catch (Exception e) {
                        logger.warn("failed to add {}, address {}", e, address);
                    }
                }
            }
        } catch (TextParseException e) {
            logger.warn("Unable to parse DNS query '{}'", query);
            logger.debug("DNS lookup exception:", e);
        }


        logger.debug("using dynamic discovery nodes {}", discoNodes);

        return discoNodes;
    }
}
