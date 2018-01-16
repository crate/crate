/*
 * This file is part of a module with proprietary Enterprise Features.
 *
 * Licensed to Crate.io Inc. ("Crate.io") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 *
 * To use this file, Crate.io must have given you permission to enable and
 * use such Enterprise Features and you must have a valid Enterprise or
 * Subscription Agreement with Crate.io.  If you enable or use the Enterprise
 * Features, you represent and warrant that you have a valid Enterprise or
 * Subscription Agreement with Crate.io.  Your use of the Enterprise Features
 * if governed by the terms and conditions of your Enterprise or Subscription
 * Agreement with Crate.io.
 */

package io.crate.auth;

import io.crate.operation.user.User;
import io.crate.operation.user.UserLookup;
import io.crate.protocols.SSL;
import io.crate.protocols.postgres.ConnectionProperties;
import org.elasticsearch.common.settings.SecureString;

import javax.annotation.Nullable;
import java.security.cert.Certificate;
import java.util.Objects;

public class ClientCertAuth implements AuthenticationMethod {

    static final String NAME = "cert";
    private final UserLookup userLookup;

    ClientCertAuth(UserLookup userLookup) {
        this.userLookup = userLookup;
    }

    @Nullable
    @Override
    public User authenticate(String userName, SecureString passwd, ConnectionProperties connProperties) {
        Certificate clientCert = connProperties.clientCert();
        if (clientCert != null) {
            String commonName = SSL.extractCN(clientCert);
            if (Objects.equals(userName, commonName)) {
                User user = userLookup.findUser(userName);
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
