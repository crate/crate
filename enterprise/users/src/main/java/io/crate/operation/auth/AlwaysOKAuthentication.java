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

package io.crate.operation.auth;

import io.crate.operation.user.UserLookup;
import io.crate.protocols.postgres.ConnectionProperties;
import org.elasticsearch.common.inject.Inject;

/**
 * Fallback if host-based authentication is disabled
 * and the user management is enabled
 */
public class AlwaysOKAuthentication implements Authentication {

    private final UserLookup userLookup;

    @Inject
    public AlwaysOKAuthentication(UserLookup userLookup) {
        this.userLookup = userLookup;
    }

    @Override
    public AuthenticationMethod resolveAuthenticationType(String user, ConnectionProperties connectionProperties) {
        return new TrustAuthenticationMethod(userLookup);
    }
}
