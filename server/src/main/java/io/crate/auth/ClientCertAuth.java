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

package io.crate.auth;

import java.security.cert.Certificate;
import java.util.Objects;

import org.elasticsearch.common.settings.SecureString;
import org.jetbrains.annotations.Nullable;

import io.crate.protocols.SSL;
import io.crate.protocols.postgres.ConnectionProperties;
import io.crate.role.Role;
import io.crate.role.RoleLookup;

public class ClientCertAuth implements AuthenticationMethod {

    static final String NAME = "cert";
    private final RoleLookup userLookup;

    ClientCertAuth(RoleLookup userLookup) {
        this.userLookup = userLookup;
    }

    @Nullable
    @Override
    public Role authenticate(String userName, SecureString passwd, ConnectionProperties connProperties) {
        Certificate clientCert = connProperties.clientCert();
        if (clientCert != null) {
            String commonName = SSL.extractCN(clientCert);
            if (Objects.equals(userName, commonName) || connProperties.protocol() == Protocol.TRANSPORT) {
                Role user = userLookup.findUser(userName);
                if (user != null) {
                    return user;
                }
            } else {
                throw new RuntimeException(
                    "Common name \"" + commonName + "\" in client certificate doesn't match username \"" + userName + "\"");
            }
        }
        throw new RuntimeException("Client certificate authentication failed for user \"" + userName + "\"");
    }

    @Override
    public String name() {
        return NAME;
    }
}
