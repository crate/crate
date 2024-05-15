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

package org.elasticsearch.transport;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.StreamCorruptedException;
import java.net.InetSocketAddress;
import java.util.Collections;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.network.CloseableChannel;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.hamcrest.Matcher;

public class TcpTransportTest extends ESTestCase {

    /** Test ipv4 host with a default port works */
    public void testParseV4DefaultPort() throws Exception {
        TransportAddress[] addresses = TcpTransport.parse("127.0.0.1", 1234);
        assertEquals(1, addresses.length);

        assertEquals("127.0.0.1", addresses[0].getAddress());
        assertEquals(1234, addresses[0].getPort());
    }

    /** Test ipv4 host with port works */
    public void testParseV4WithPort() throws Exception {
        TransportAddress[] addresses = TcpTransport.parse("127.0.0.1:2345", 1234);
        assertEquals(1, addresses.length);

        assertEquals("127.0.0.1", addresses[0].getAddress());
        assertEquals(2345, addresses[0].getPort());
    }

    /** Test unbracketed ipv6 hosts in configuration fail. Leave no ambiguity */
    public void testParseV6UnBracketed() throws Exception {
        try {
            TcpTransport.parse("::1", 1234);
            fail("should have gotten exception");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("must be bracketed"));
        }
    }

    /** Test ipv6 host with a default port works */
    public void testParseV6DefaultPort() throws Exception {
        TransportAddress[] addresses = TcpTransport.parse("[::1]", 1234);
        assertEquals(1, addresses.length);

        assertEquals("::1", addresses[0].getAddress());
        assertEquals(1234, addresses[0].getPort());
    }

    /** Test ipv6 host with port works */
    public void testParseV6WithPort() throws Exception {
        TransportAddress[] addresses = TcpTransport.parse("[::1]:2345", 1234);
        assertEquals(1, addresses.length);

        assertEquals("::1", addresses[0].getAddress());
        assertEquals(2345, addresses[0].getPort());
    }

    public void testRejectsPortRanges() {
        assertThatThrownBy(() -> TcpTransport.parse("[::1]:100-200", 1000))
            .isExactlyInstanceOf(NumberFormatException.class);
    }

    public void testDefaultSeedAddressesWithDefaultPort() {
        testDefaultSeedAddresses(Settings.EMPTY, containsInAnyOrder(
            "[::1]:4300", "[::1]:4301", "[::1]:4302", "[::1]:4303", "[::1]:4304", "[::1]:4305",
            "127.0.0.1:4300", "127.0.0.1:4301", "127.0.0.1:4302", "127.0.0.1:4303", "127.0.0.1:4304", "127.0.0.1:4305"));
    }

    public void testDefaultSeedAddressesWithNonstandardGlobalPortRange() {
        testDefaultSeedAddresses(Settings.builder().put(TransportSettings.PORT.getKey(), "4500-4600").build(), containsInAnyOrder(
            "[::1]:4500", "[::1]:4501", "[::1]:4502", "[::1]:4503", "[::1]:4504", "[::1]:4505",
            "127.0.0.1:4500", "127.0.0.1:4501", "127.0.0.1:4502", "127.0.0.1:4503", "127.0.0.1:4504", "127.0.0.1:4505"));
    }

    public void testDefaultSeedAddressesWithSmallGlobalPortRange() {
        testDefaultSeedAddresses(Settings.builder().put(TransportSettings.PORT.getKey(), "4300-4302").build(), containsInAnyOrder(
            "[::1]:4300", "[::1]:4301", "[::1]:4302",
            "127.0.0.1:4300", "127.0.0.1:4301", "127.0.0.1:4302"));
    }


    public void testDefaultSeedAddressesWithNonstandardSinglePort() {
        testDefaultSeedAddresses(Settings.builder().put(TransportSettings.PORT.getKey(), "4500").build(),
                                 containsInAnyOrder("[::1]:4500", "127.0.0.1:4500"));
    }

    public void testTLSHeader() throws IOException {
        BytesStreamOutput streamOutput = new BytesStreamOutput(1 << 14);

        streamOutput.write(0x16);
        streamOutput.write(0x03);
        byte byte1 = randomByte();
        streamOutput.write(byte1);
        byte byte2 = randomByte();
        streamOutput.write(byte2);
        streamOutput.write(randomByte());
        streamOutput.write(randomByte());
        streamOutput.write(randomByte());

        try {
            TcpTransport.readMessageLength(streamOutput.bytes());
            fail("Expected exception");
        } catch (Exception ex) {
            assertThat(ex).isExactlyInstanceOf(StreamCorruptedException.class);
            String expected = "SSL/TLS request received but SSL/TLS is not enabled on this node, got (16,3,"
                + Integer.toHexString(byte1 & 0xFF) + ","
                + Integer.toHexString(byte2 & 0xFF) + ")";
            assertEquals(expected, ex.getMessage());
        }
    }

    private void testDefaultSeedAddresses(final Settings settings, Matcher<Iterable<? extends String>> seedAddressesMatcher) {
        final TestThreadPool testThreadPool = new TestThreadPool("test");
        try {
            final TcpTransport tcpTransport = new TcpTransport(settings,
                                                               Version.CURRENT,
                                                               testThreadPool,
                                                               new PageCacheRecycler(Settings.EMPTY),
                                                               new NoneCircuitBreakerService(),
                                                               writableRegistry(),
                                                               new NetworkService(Collections.emptyList())) {

                @Override
                protected CloseableChannel bind(InetSocketAddress address) {
                    throw new UnsupportedOperationException();
                }

                @Override
                protected ConnectResult initiateChannel(DiscoveryNode node) {
                    throw new UnsupportedOperationException();
                }

                @Override
                protected void stopInternal() {
                    throw new UnsupportedOperationException();
                }
            };

            assertThat(tcpTransport.getDefaultSeedAddresses(), seedAddressesMatcher);
        } finally {
            testThreadPool.shutdown();
        }
    }
}
