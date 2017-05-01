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

package io.crate.operation.user;

import io.crate.operation.collect.sources.SysTableRegistry;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.concurrent.CompletableFuture;

public class UserManagerServiceFactory implements UserManagerFactory {

    @Override
    public UserManager create(Settings settings,
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
        UserManagerService userManagerService = new UserManagerService(transportCreateAction, transportDropUserAction);

        sysTableRegistry.registerSysTable(new SysUsersTableInfo(clusterService),
            () -> CompletableFuture.completedFuture(userManagerService.users()),
            SysUsersTableInfo.sysUsersExpressions());
        return userManagerService;
    }

}
