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


package io.crate.protocols.ssl;

import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLContext;


/**
 * Part of the logic from this file is from
 *
 * https://github.com/opendistro-for-elasticsearch/security/blob/main/src/main/java/com/amazon/opendistroforelasticsearch/security/ssl/util/SSLConnectionTestUtil.java
 *
 **/
public final class ConnectionTest {

    public static final String DUAL_MODE_CLIENT_HELLO_MSG = "DUALCM";
    public static final String DUAL_MODE_SERVER_HELLO_MSG = "DUALSM";


    public enum ProbeResult {
        SSL_AVAILABLE,
        SSL_MISSING,
    }

    public static ProbeResult probeDualMode(InetSocketAddress address) {
        return probeDualMode(address.getHostName(), address.getPort());
    }

    public static ProbeResult probeDualMode(String host, int port) {
        // https://docs.oracle.com/javase/8/docs/api/java/net/Socket.html#close--
        // Closing this socket will also close the socket's InputStream and OutputStream.
        // One try-with-resources is enough as it releases all resources.
        try (var socket = new Socket(host, port)) {
            socket.setSoTimeout((int) TimeUnit.SECONDS.toMillis(10));

            var writer = new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8);
            writer.write(DUAL_MODE_CLIENT_HELLO_MSG);
            writer.flush();

            var reader = new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8);
            StringBuilder response = new StringBuilder();
            int c;
            while ((c = reader.read()) != -1) {
                response.append((char) c);
            }

            if (response.toString().equals(DUAL_MODE_SERVER_HELLO_MSG)) {
                return ProbeResult.SSL_AVAILABLE;
            }
        } catch (Exception ignored) {
            return ProbeResult.SSL_MISSING;
        }
        return ProbeResult.SSL_MISSING;
    }

    public static ProbeResult probeSSL(SSLContext sslContext, InetSocketAddress address) {
        var socketFactory = sslContext.getSocketFactory();
        try (var socket = socketFactory.createSocket(address.getHostName(), address.getPort())) {
            // need to write something to trigger SSL handshake
            try (var out = socket.getOutputStream()) {
                out.write(1);
            }
            return ProbeResult.SSL_AVAILABLE;
        } catch (Exception ignored) {
            return ProbeResult.SSL_MISSING;
        }
    }
}
