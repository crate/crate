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

package io.crate.statistics;


import io.crate.action.sql.BaseResultReceiver;
import io.crate.action.sql.SQLOperations;
import io.crate.action.sql.Session;
import io.crate.common.annotations.VisibleForTesting;
import io.crate.common.unit.TimeValue;
import io.crate.data.Row;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.Statement;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import javax.annotation.Nullable;

/**
 * Periodically refresh {@link TableStats} based on {@link #refreshInterval}.
 */
@Singleton
public class TableStatsService implements Runnable {

    private static final Logger LOGGER = LogManager.getLogger(TableStatsService.class);

    public static final Setting<TimeValue> STATS_SERVICE_REFRESH_INTERVAL_SETTING = Setting.timeSetting(
        "stats.service.interval", TimeValue.timeValueHours(24), Setting.Property.NodeScope, Setting.Property.Dynamic);

    static final String STMT = "ANALYZE";
    private static final Statement PARSED_STMT = SqlParser.createStatement(STMT);

    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final Session session;

    @VisibleForTesting
    volatile TimeValue refreshInterval;

    @VisibleForTesting
    volatile Scheduler.ScheduledCancellable scheduledRefresh;

    @Inject
    public TableStatsService(Settings settings,
                             ThreadPool threadPool,
                             ClusterService clusterService,
                             SQLOperations sqlOperations) {
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        refreshInterval = STATS_SERVICE_REFRESH_INTERVAL_SETTING.get(settings);
        scheduledRefresh = scheduleNextRefresh(refreshInterval);
        session = sqlOperations.newSystemSession();

        clusterService.getClusterSettings().addSettingsUpdateConsumer(
            STATS_SERVICE_REFRESH_INTERVAL_SETTING, this::setRefreshInterval);
    }

    @Override
    public void run() {
        updateStats();
    }

    public void updateStats() {
        if (clusterService.localNode() == null) {
            /*
              During a long startup (e.g. during an upgrade process) the localNode() may be null
              and this would lead to NullPointerException in the TransportExecutor.
             */
            LOGGER.debug("Could not retrieve table stats. localNode is not fully available yet.");
            return;
        }
        if (!clusterService.state().nodes().isLocalNodeElectedMaster()) {
            // `ANALYZE` will publish the new stats to all nodes, so we need only a single node running it.
            return;
        }
        try {
            BaseResultReceiver resultReceiver = new BaseResultReceiver();
            resultReceiver.completionFuture().whenComplete((res, err) -> {
                scheduledRefresh = scheduleNextRefresh(refreshInterval);
                if (err != null) {
                    LOGGER.error("Error running periodic " + STMT + "", err);
                }
            });
            session.quickExec(STMT, stmt -> PARSED_STMT, resultReceiver, Row.EMPTY);
        } catch (Throwable t) {
            LOGGER.error("error retrieving table stats", t);
        }
    }

    @Nullable
    private Scheduler.ScheduledCancellable scheduleNextRefresh(TimeValue refreshInterval) {
        if (refreshInterval.millis() > 0) {
            return threadPool.schedule(
                this,
                refreshInterval,
                ThreadPool.Names.REFRESH
            );
        }
        return null;
    }

    private void setRefreshInterval(TimeValue newRefreshInterval) {
        if (scheduledRefresh != null) {
            scheduledRefresh.cancel();
            scheduledRefresh = null;
        }
        refreshInterval = newRefreshInterval;
        scheduledRefresh = scheduleNextRefresh(newRefreshInterval);
    }
}

