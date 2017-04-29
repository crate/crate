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

package io.crate.operation.auth;

import org.elasticsearch.cluster.service.ClusterService;
import com.google.common.annotations.VisibleForTesting;
import io.crate.action.sql.SessionContext;
import io.crate.protocols.postgres.Messages;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;
import org.jboss.netty.channel.Channel;

import java.net.InetAddress;
import java.util.Iterator;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.concurrent.CompletableFuture;

@Singleton
public class AuthenticationProvider {

    private Authentication authService;
    @VisibleForTesting
    public static final Authentication NOOP_AUTH = new Authentication() {

        private final AuthenticationMethod alwaysOk = new AuthenticationMethod() {
            @Override
            public CompletableFuture<Boolean> pgAuthenticate(Channel channel, SessionContext session) {
                CompletableFuture<Boolean> future = new CompletableFuture<>();
                Messages.sendAuthenticationOK(channel)
                    .addListener(f -> future.complete(true));
                return future;
            }

            @Override
            public String name() {
                return "alwaysOk";
            }
        };

        @Override
        public boolean enabled() {
            return true;
        }

        @Override
        public AuthenticationMethod resolveAuthenticationType(String user, InetAddress address) {
            return alwaysOk;
        }
    };

    @Inject
    public AuthenticationProvider(ClusterService clusterService,
                                  Settings settings) {
        UserServiceFactory serviceFactory = getUserServiceFactory();
        authService = serviceFactory == null ? NOOP_AUTH : serviceFactory.authService(clusterService, settings);
    }

    private static UserServiceFactory getUserServiceFactory() {
        Iterator<UserServiceFactory> authIterator = ServiceLoader.load(UserServiceFactory.class).iterator();
        UserServiceFactory factory = null;
        while (authIterator.hasNext()) {
            if (factory != null) {
                throw new ServiceConfigurationError("UserManagerFactory found twice");
            }
            factory = authIterator.next();
        }
        return factory;
    }

    public Authentication authService() {
        // fallback to NOOP_AUTH which responds always OK
        // if the found authentication service is not enabled
        return authService.enabled() ? authService : NOOP_AUTH;
    }
}
