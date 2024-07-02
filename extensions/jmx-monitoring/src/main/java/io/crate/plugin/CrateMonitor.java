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

package io.crate.plugin;

import java.lang.management.ManagementFactory;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.jetbrains.annotations.Nullable;

import io.crate.action.sql.Sessions;
import io.crate.beans.CircuitBreakers;
import io.crate.beans.Connections;
import io.crate.beans.NodeInfo;
import io.crate.beans.NodeStatus;
import io.crate.beans.QueryStats;
import io.crate.beans.ThreadPools;
import io.crate.execution.engine.collect.stats.JobsLogs;
import io.crate.protocols.postgres.PostgresNetty;

public class CrateMonitor {

    private final Logger logger;
    private final MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();

    @Inject
    public CrateMonitor(JobsLogs jobsLogs,
                        PostgresNetty postgresNetty,
                        @Nullable HttpServerTransport httpServerTransport,
                        TransportService transportService,
                        Sessions sqlOperations,
                        ClusterService clusterService,
                        ThreadPool threadPool,
                        CircuitBreakerService breakerService,
                        IndicesService indicesService) {
        logger = LogManager.getLogger(CrateMonitor.class);
        registerMBean(QueryStats.NAME, new QueryStats(jobsLogs));
        registerMBean(NodeStatus.NAME, new NodeStatus(sqlOperations::isEnabled));
        registerMBean(NodeInfo.NAME, new NodeInfo(clusterService::state, new NodeInfo.ShardStateAndSizeProvider(indicesService)));
        registerMBean(Connections.NAME, new Connections(
            () -> httpServerTransport == null ? null : httpServerTransport.stats(),
            postgresNetty::stats,
            transportService::stats
        ));
        registerMBean(ThreadPools.NAME, new ThreadPools(threadPool));
        registerMBean(CircuitBreakers.NAME, new CircuitBreakers(breakerService));
    }

    private void registerMBean(String name, Object bean) {
        try {
            mbeanServer.registerMBean(bean, new ObjectName(name));
        } catch (InstanceAlreadyExistsException | NotCompliantMBeanException |
            MBeanRegistrationException | MalformedObjectNameException e) {
            logger.error("The MBean: {} cannot be registered: {}", name, e);
        }
    }
}
