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

package io.crate.operation.user;

import io.crate.concurrent.CompletableFutures;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.metadata.sys.SysSchemaInfo;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Iterator;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.concurrent.CompletableFuture;

@Singleton
public class UserManagerProvider implements Provider<UserManager> {

    private final UserManager userManager;

    @Inject
    public UserManagerProvider(Settings settings,
                               TransportService transportService,
                               ClusterService clusterService,
                               ThreadPool threadPool,
                               ActionFilters actionFilters,
                               IndexNameExpressionResolver indexNameExpressionResolver,
                               SysSchemaInfo sysSchemaInfo) {
        Iterator<UserManagerFactory> userManagerIterator = ServiceLoader.load(UserManagerFactory.class).iterator();
        UserManagerFactory userManagerFactory = null;
        while (userManagerIterator.hasNext()) {
            if (userManagerFactory != null) {
                throw new ServiceConfigurationError("UserManagerFactory found twice");
            }
            userManagerFactory = userManagerIterator.next();
        }
        if (userManagerFactory == null) {
            this.userManager = new UnsupportedUserManager();
        } else {
            this.userManager = userManagerFactory.create(settings, transportService, clusterService, threadPool,
                actionFilters, indexNameExpressionResolver, sysSchemaInfo);
        }
    }


    @Override
    public UserManager get() {
        return userManager;
    }

    private static class UnsupportedUserManager implements UserManager {

        @Override
        public CompletableFuture<Long> createUser(String userName) {
            return CompletableFutures.failedFuture(
                new UnsupportedFeatureException("CREATE USER is only supported in enterprise version")
            );
        }
    }
}
