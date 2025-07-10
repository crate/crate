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


package io.crate.test.utils;

import java.net.InetSocketAddress;

import javax.net.ssl.SSLContext;


public final class ConnectionTestUtil {

    private ConnectionTestUtil() {}

    public enum ProbeResult {
        SSL_AVAILABLE,
        SSL_MISSING,
    }

    public static ProbeResult probeSSL(SSLContext sslContext, InetSocketAddress address) {
        var socketFactory = sslContext.getSocketFactory();
        try (var socket = socketFactory.createSocket(address.getHostName(), address.getPort())) {
            // need to write something to trigger SSL handshake
            var out = socket.getOutputStream(); // Closed by outer try-with-resources.
            out.write(1);

            return ProbeResult.SSL_AVAILABLE;
        } catch (Exception _) {
            return ProbeResult.SSL_MISSING;
        }
    }
}
