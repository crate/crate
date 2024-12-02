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

import static org.assertj.core.api.Assertions.anyOf;
import static org.assertj.core.api.Assertions.assertThat;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.assertj.core.api.Condition;
import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.dns.DefaultDnsRawRecord;
import io.netty.handler.codec.dns.DefaultDnsRecordEncoder;
import io.netty.handler.codec.dns.DnsRecord;
import io.netty.handler.codec.dns.DnsRecordType;

public class SrvUnicastHostsProviderTest extends ESTestCase {

    private ThreadPool threadPool;
    private SrvUnicastHostsProvider srvUnicastHostsProvider;
    private Condition<String> isLocalHost;

    @Before
    public void mockTransportService() throws Exception {
        String localHostName = InetAddress.getLocalHost().getCanonicalHostName();
        isLocalHost = anyOf(
            new Condition<>(localHostName::equals, localHostName),
            new Condition<>("localhost"::equals, "localhost"),
            new Condition<>("127.0.0.1"::equals, "127.0.0.1"));
        threadPool = new TestThreadPool("dummy", Settings.EMPTY);
        TransportService transportService = MockTransportService.createNewService(
            Settings.EMPTY, Version.CURRENT, threadPool, null);
        srvUnicastHostsProvider = new SrvUnicastHostsProvider(Settings.EMPTY, transportService);
    }

    @After
    public void releaseResources() throws Exception {
        srvUnicastHostsProvider.close();
        srvUnicastHostsProvider.eventLoopGroup().shutdownGracefully();
        srvUnicastHostsProvider.eventLoopGroup().awaitTermination(30, TimeUnit.SECONDS);
        threadPool.shutdown();
        threadPool.awaitTermination(30, TimeUnit.SECONDS);
    }

    @Test
    public void testParseAddressNoPort() {
        Settings settings = Settings.builder()
            .put(SrvUnicastHostsProvider.DISCOVERY_SRV_RESOLVER.getKey(), "127.0.0.1")
            .build();
        InetSocketAddress address = srvUnicastHostsProvider.parseResolverAddress(settings);
        assertThat(address.getHostName()).satisfies(isLocalHost);
        assertThat(address.getPort()).isEqualTo(53);
    }

    @Test
    public void testParseAddressValidPort() {
        Settings settings = Settings.builder()
            .put(SrvUnicastHostsProvider.DISCOVERY_SRV_RESOLVER.getKey(), "127.0.0.1:1234")
            .build();
        InetSocketAddress address = srvUnicastHostsProvider.parseResolverAddress(settings);
        assertThat(address.getHostName()).satisfies(isLocalHost);
        assertThat(address.getPort()).isEqualTo(1234);
    }

    @Test
    public void testParseAddressPortOutOfRange() {
        Settings settings = Settings.builder()
            .put(SrvUnicastHostsProvider.DISCOVERY_SRV_RESOLVER.getKey(), "127.0.0.1:1234567")
            .build();
        InetSocketAddress address = srvUnicastHostsProvider.parseResolverAddress(settings);
        assertThat(address.getHostName()).satisfies(isLocalHost);
        assertThat(address.getPort()).isEqualTo(53);
    }

    @Test
    public void testParseAddressPortNoInteger() {
        Settings settings = Settings.builder()
            .put(SrvUnicastHostsProvider.DISCOVERY_SRV_RESOLVER.getKey(), "127.0.0.1:foo")
            .build();
        InetSocketAddress address = srvUnicastHostsProvider.parseResolverAddress(settings);
        assertThat(address.getHostName()).satisfies(isLocalHost);
        assertThat(address.getPort()).isEqualTo(53);
    }

    @Test
    public void testParseRecords() {
        ByteBuf buf = Unpooled.buffer();
        buf.writeShort(0); // priority
        buf.writeShort(0); // weight
        buf.writeShort(993); // port
        encodeName("localhost.", buf);
        DnsRecord record = new DefaultDnsRawRecord("_myprotocol._tcp.crate.io.", DnsRecordType.SRV, 30, buf);

        List<TransportAddress> addresses = srvUnicastHostsProvider.parseRecords(Collections.singletonList(record));
        assertThat(addresses.get(0).getAddress()).isEqualTo("127.0.0.1");
        assertThat(addresses.get(0).getPort()).isEqualTo(993);
    }

    /**
     * Copied over from {@link DefaultDnsRecordEncoder#encodeName(String, ByteBuf)} as it is not accessible.
     */
    private void encodeName(String name, ByteBuf buf) {
        if (".".equals(name)) {
            // Root domain
            buf.writeByte(0);
            return;
        }

        final String[] labels = name.split("\\.");
        for (String label : labels) {
            final int labelLen = label.length();
            if (labelLen == 0) {
                // zero-length label means the end of the name.
                break;
            }

            buf.writeByte(labelLen);
            ByteBufUtil.writeAscii(buf, label);
        }

        buf.writeByte(0); // marks end of name field
    }
}
