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

package io.crate.plugin;

import io.crate.action.sql.SQLOperations;
import io.crate.beans.Connections;
import io.crate.beans.NodeInfo;
import io.crate.beans.NodeStatus;
import io.crate.beans.QueryStats;
import io.crate.beans.ThreadPools;
import io.crate.execution.engine.collect.stats.JobsLogs;
import io.crate.protocols.ConnectionStats;
import io.crate.protocols.postgres.PostgresNetty;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import javax.annotation.Nullable;
import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;

public class CrateMonitor {

    private final Logger logger;
    private final MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();

    @Inject
    public CrateMonitor(JobsLogs jobsLogs,
                        Settings settings,
                        PostgresNetty postgresNetty,
                        @Nullable HttpServerTransport httpServerTransport,
                        TransportService transportService,
                        SQLOperations sqlOperations,
                        ClusterService clusterService,
                        ThreadPool threadPool) {
        logger = Loggers.getLogger(CrateMonitor.class, settings);
        registerMBean(QueryStats.NAME, new QueryStats(jobsLogs));
        registerMBean(NodeStatus.NAME, new NodeStatus(sqlOperations::isEnabled));
        registerMBean(NodeInfo.NAME, new NodeInfo(clusterService::localNode));
        registerMBean(Connections.NAME, new Connections(
            () -> httpServerTransport == null ? null : httpServerTransport.stats(),
            () -> new ConnectionStats(postgresNetty.openConnections(), postgresNetty.totalConnections()),
            () -> transportService.stats().serverOpen()
        ));
        registerMBean(ThreadPools.NAME, new ThreadPools(threadPool));
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
