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

package io.crate.protocols.postgres;

import io.crate.auth.Protocol;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import org.jetbrains.annotations.Nullable;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import java.net.InetAddress;
import java.security.cert.Certificate;

public class ConnectionProperties {

    private static final Logger LOGGER = LogManager.getLogger(ConnectionProperties.class);

    private final InetAddress address;
    private final Protocol protocol;
    private final boolean hasSSL;

    @Nullable
    private final SSLSession sslSession;

    public ConnectionProperties(InetAddress address, Protocol protocol, @Nullable SSLSession sslSession) {
        this.address = address;
        this.protocol = protocol;
        this.hasSSL = sslSession != null;
        this.sslSession = sslSession;
    }

    public boolean hasSSL() {
        return hasSSL;
    }

    public InetAddress address() {
        return address;
    }

    public Protocol protocol() {
        return protocol;
    }

    public Certificate clientCert() {
        // This logic isn't in the constructor to prevent logging in case of SSL without (expected) client-certificate auth
        if (sslSession != null) {
            try {
                return sslSession.getPeerCertificates()[0];
            } catch (ArrayIndexOutOfBoundsException | SSLPeerUnverifiedException e) {
                LOGGER.debug("Client certificate not available", e);
            }
        }
        return null;
    }
}
