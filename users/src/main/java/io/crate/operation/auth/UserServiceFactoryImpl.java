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

import io.crate.http.netty.HttpAuthUpstreamHandler;
import io.crate.metadata.sys.SysUsersTableInfo;
import io.crate.metadata.sys.SysPrivilegesTableInfo;
import io.crate.operation.collect.sources.SysTableRegistry;
import io.crate.operation.user.TransportCreateUserAction;
import io.crate.operation.user.TransportDropUserAction;
import io.crate.operation.user.TransportPrivilegesAction;
import io.crate.operation.user.UserManager;
import io.crate.operation.user.UserManagerService;
import io.crate.plugin.PipelineRegistry;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.concurrent.CompletableFuture;


public class UserServiceFactoryImpl implements UserServiceFactory {

    @Override
    public Authentication authService(Settings settings, UserManager userManager) {
        return new HostBasedAuthentication(settings, userManager);
    }

    /**
     *  This mus only be called once as TransportActions are registered here.
     */
    @Override
    public UserManager setupUserManager(Settings settings,
                                        TransportService transportService,
                                        ClusterService clusterService,
                                        ThreadPool threadPool,
                                        ActionFilters actionFilters,
                                        IndexNameExpressionResolver indexNameExpressionResolver,
                                        SysTableRegistry sysTableRegistry) {
        TransportCreateUserAction transportCreateAction = new TransportCreateUserAction(
            settings, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver);
        TransportDropUserAction transportDropUserAction = new TransportDropUserAction(
            settings, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver);
        TransportPrivilegesAction transportPrivilegesAction = new TransportPrivilegesAction(
            settings, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver);
        UserManagerService userManager = new UserManagerService(
            transportCreateAction, transportDropUserAction, transportPrivilegesAction, clusterService);
        sysTableRegistry.registerSysTable(new SysUsersTableInfo(clusterService),
            () -> CompletableFuture.completedFuture(userManager.users()),
            SysUsersTableInfo.sysUsersExpressions());

        sysTableRegistry.registerSysTable(new SysPrivilegesTableInfo(clusterService),
            () -> CompletableFuture.completedFuture(SysPrivilegesTableInfo.buildPrivilegesRows(userManager.users())),
            SysPrivilegesTableInfo.expressions());

        return userManager;
    }

    @Override
    public void registerHttpAuthHandler(Settings settings, PipelineRegistry pipelineRegistry, Authentication authService) {
        PipelineRegistry.ChannelPipelineItem pipelineItem = new PipelineRegistry.ChannelPipelineItem(
            "blob_handler",
            "auth_handler",
            () -> new HttpAuthUpstreamHandler(settings, authService)
        );
        pipelineRegistry.addBefore(pipelineItem);
    }
}
