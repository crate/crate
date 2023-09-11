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

package io.crate.discovery;

import static org.elasticsearch.common.util.concurrent.EsExecutors.daemonThreadFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.discovery.SeedHostsProvider;
import org.elasticsearch.transport.TransportService;

import io.crate.common.unit.TimeValue;
import io.netty.buffer.ByteBuf;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.codec.dns.DefaultDnsQuestion;
import io.netty.handler.codec.dns.DefaultDnsRawRecord;
import io.netty.handler.codec.dns.DefaultDnsRecordDecoder;
import io.netty.handler.codec.dns.DnsRecord;
import io.netty.handler.codec.dns.DnsRecordType;
import io.netty.resolver.dns.DnsNameResolver;
import io.netty.resolver.dns.DnsNameResolverBuilder;
import io.netty.resolver.dns.SingletonDnsServerAddressStreamProvider;
import io.netty.util.ReferenceCounted;

@Singleton
public class SrvUnicastHostsProvider implements AutoCloseable, SeedHostsProvider {

    private static final Logger LOGGER = LogManager.getLogger(SrvUnicastHostsProvider.class);

    public static final Setting<String> DISCOVERY_SRV_QUERY = Setting.simpleString(
        "discovery.srv.query", Setting.Property.NodeScope);
    public static final Setting<String> DISCOVERY_SRV_RESOLVER = Setting.simpleString(
        "discovery.srv.resolver", Setting.Property.NodeScope);
    private static final Setting<TimeValue> DISCOVERY_SRV_RESOLVE_TIMEOUT =
        Setting.positiveTimeSetting("discovery.srv.resolve_timeout", TimeValue.timeValueSeconds(5), Setting.Property.NodeScope);

    private final TransportService transportService;
    private final String query;
    private final TimeValue resolveTimeout;
    private final DnsNameResolver resolver;
    private final EventLoopGroup eventLoopGroup;

    @Inject
    public SrvUnicastHostsProvider(Settings settings, TransportService transportService) {
        this.transportService = transportService;
        this.query = DISCOVERY_SRV_QUERY.get(settings);
        this.resolveTimeout = DISCOVERY_SRV_RESOLVE_TIMEOUT.get(settings);

        eventLoopGroup = new NioEventLoopGroup(1, daemonThreadFactory(settings, "netty-dns-resolver"));
        resolver = buildResolver(settings);
    }

    EventLoopGroup eventLoopGroup() {
        return eventLoopGroup;
    }

    InetSocketAddress parseResolverAddress(Settings settings) {
        String hostname;
        int port = 53;
        String resolverAddress = DISCOVERY_SRV_RESOLVER.get(settings);
        if (Strings.hasLength(resolverAddress)) {
            String[] parts = resolverAddress.split(":");
            if (parts.length > 0) {
                hostname = parts[0];
                if (parts.length > 1) {
                    try {
                        port = Integer.parseInt(parts[1]);
                    } catch (Exception e) {
                        LOGGER.warn("Resolver port '{}' is not an integer. Using default port 53", parts[1]);
                    }
                }
                try {
                    return new InetSocketAddress(hostname, port);
                } catch (IllegalArgumentException e) {
                    LOGGER.warn("Resolver port '{}' is out of range. Using default port 53", parts[1]);
                    return new InetSocketAddress(hostname, 53);
                }
            }
        }
        return null;
    }

    private DnsNameResolver buildResolver(Settings settings) {
        DnsNameResolverBuilder resolverBuilder = new DnsNameResolverBuilder(eventLoopGroup.next());
        resolverBuilder.channelType(NioDatagramChannel.class);

        InetSocketAddress resolverAddress = parseResolverAddress(settings);
        if (resolverAddress != null) {
            try {
                resolverBuilder.nameServerProvider(new SingletonDnsServerAddressStreamProvider(resolverAddress));
            } catch (IllegalArgumentException e) {
                LOGGER.warn("Could not create custom dns resolver. Using default resolver.", e);
            }
        }
        return resolverBuilder.build();
    }

    @Override
    public List<TransportAddress> getSeedAddresses(HostsResolver hostsResolver) {
        if (query == null) {
            LOGGER.error("DNS query must not be null. Please set '{}'", DISCOVERY_SRV_QUERY);
            return Collections.emptyList();
        }
        try {
            List<DnsRecord> records = lookupRecords();
            try {
                LOGGER.trace("Building dynamic unicast discovery nodes...");
                if (records == null || records.size() == 0) {
                    LOGGER.debug("No nodes found");
                } else {
                    List<TransportAddress> transportAddresses = parseRecords(records);
                    LOGGER.info("Using dynamic nodes {}", transportAddresses);
                    return transportAddresses;
                }
            } finally {
                for (var record : records) {
                    if (record instanceof ReferenceCounted) {
                        ((ReferenceCounted) record).release();
                    }
                }
            }
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            LOGGER.error("DNS lookup exception:", e);
        }
        return Collections.emptyList();
    }

    private List<DnsRecord> lookupRecords() throws InterruptedException, ExecutionException, TimeoutException {
        return resolver.resolveAll(new DefaultDnsQuestion(query, DnsRecordType.SRV), Collections.emptyList())
            .get(resolveTimeout.millis(), TimeUnit.MILLISECONDS);
    }

    List<TransportAddress> parseRecords(List<DnsRecord> records) {
        List<TransportAddress> addresses = new ArrayList<>(records.size());
        for (DnsRecord record : records) {
            if (record instanceof DefaultDnsRawRecord) {
                DefaultDnsRawRecord rawRecord = (DefaultDnsRawRecord) record;
                ByteBuf content = rawRecord.content();
                // first is "priority", we don't use it
                content.readUnsignedShort();
                // second is "weight", we don't use it
                content.readUnsignedShort();
                int port = content.readUnsignedShort();
                String hostname = DefaultDnsRecordDecoder.decodeName(content).replaceFirst("\\.$", "");
                String address = hostname + ":" + port;
                try {
                    for (TransportAddress transportAddress : transportService.addressesFromString(address)) {
                        if (LOGGER.isTraceEnabled()) {
                            LOGGER.trace("adding {}, transport_address {}", address, transportAddress);
                        }
                        addresses.add(transportAddress);
                    }
                } catch (Exception e) {
                    LOGGER.warn("failed to add " + address, e);
                }
            }
        }
        return addresses;
    }

    @Override
    public void close() {
        resolver.close();
    }

}
