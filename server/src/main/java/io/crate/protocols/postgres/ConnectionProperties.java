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

import java.net.InetAddress;
import java.security.cert.Certificate;
import java.util.ArrayList;
import java.util.List;

import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;

import org.jetbrains.annotations.Nullable;

import io.crate.auth.Credentials;
import io.crate.auth.Protocol;

public class ConnectionProperties {

    private final InetAddress address;
    private final Protocol protocol;
    private final boolean hasSSL;
    private final List<ClientMethod> clientMethods;
    private final Certificate certificate;

    public enum ClientMethod {
        CERT,
        PASSWORD,
        JWT,
        TRUST
    }

    public ConnectionProperties(@Nullable Credentials credentials,
                                InetAddress address,
                                Protocol protocol,
                                @Nullable SSLSession sslSession) {
        this.address = address;
        this.protocol = protocol;
        this.hasSSL = sslSession != null;
        Certificate cert = null;
        if (sslSession != null) {
            try {
                cert = sslSession.getPeerCertificates()[0];
            } catch (ArrayIndexOutOfBoundsException | SSLPeerUnverifiedException e) {
                cert = null;
            }
        }
        this.certificate = cert;
        this.clientMethods = new ArrayList<>();

        // We never resolve this method from connection properties.
        // It should be always added to match entries with explicit "trust" declaration
        clientMethods.add(ClientMethod.TRUST);

        if (certificate != null) {
            clientMethods.add(ClientMethod.CERT);
        }
        if (credentials != null) {
            if (credentials.password() != null || Protocol.POSTGRES.equals(protocol)) {
                // The PG protocol will not give any hints at this stage if password will be used, so we always enable support for it.
                clientMethods.add(ConnectionProperties.ClientMethod.PASSWORD);
            }
            if (credentials.decodedToken() != null) {
                clientMethods.add(ConnectionProperties.ClientMethod.JWT);
            }
        }
    }

    /**
     * Order of methods is not important.
     * Only HBA order matters.
     */
    public List<ClientMethod> clientMethods() {
        return clientMethods;
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

    @Nullable
    public Certificate clientCert() {
        return certificate;
    }
}
