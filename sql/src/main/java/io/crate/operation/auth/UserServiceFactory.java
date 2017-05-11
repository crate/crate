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

import io.crate.operation.collect.sources.SysTableRegistry;
import io.crate.operation.user.UserManager;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * This is a marker interface for a factory implementation that lives in the "user" module
 * which is only available for the Enterprise Edition.
 * Also, the implementations returned by this factory are part of the "user" module.
 * The interfaces, however, are part of the "sql" module.
 */
public interface UserServiceFactory {

    /**
     * Return implementation of {@link Authentication} interface.
     */
    Authentication authService(Settings settings, UserManager userManager);

    /**
     * Return implementation of {@link UserManager} interface.
     */
    UserManager setupUserManager(Settings settings,
                                 TransportService transportService,
                                 ClusterService clusterService,
                                 ThreadPool threadPool,
                                 ActionFilters actionFilters,
                                 IndexNameExpressionResolver indexNameExpressionResolver,
                                 SysTableRegistry sysTableRegistry);

}
