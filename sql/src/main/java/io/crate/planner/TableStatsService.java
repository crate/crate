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

package io.crate.planner;


import com.carrotsearch.hppc.ObjectLongHashMap;
import com.carrotsearch.hppc.ObjectLongMap;
import com.google.common.annotations.VisibleForTesting;
import io.crate.action.sql.BaseResultReceiver;
import io.crate.action.sql.SQLOperations;
import io.crate.data.Row;
import io.crate.metadata.TableIdent;
import io.crate.settings.CrateSetting;
import io.crate.types.DataTypes;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.function.Consumer;

@Singleton
public class TableStatsService extends AbstractComponent implements Runnable {

    public static final CrateSetting<TimeValue> STATS_SERVICE_REFRESH_INTERVAL_SETTING = CrateSetting.of(Setting.timeSetting(
        "stats.service.interval", TimeValue.timeValueHours(1), Setting.Property.NodeScope, Setting.Property.Dynamic),
        DataTypes.STRING);

    static final String TABLE_STATS = "table_stats";
    static final int DEFAULT_SOFT_LIMIT = 10_000;
    static final String STMT =
        "select cast(sum(num_docs) as long), schema_name, table_name from sys.shards group by 2, 3";

    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final TableStatsResultReceiver resultReceiver;
    private final SQLOperations.SQLDirectExecutor sqlDirectExecutor;

    @VisibleForTesting
    ThreadPool.Cancellable refreshScheduledTask = null;
    @VisibleForTesting
    TimeValue refreshInterval;

    @Inject
    public TableStatsService(Settings settings,
                             ThreadPool threadPool,
                             ClusterService clusterService,
                             TableStats tableStats,
                             SQLOperations sqlOperations) {
        super(settings);
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        resultReceiver = new TableStatsResultReceiver(tableStats::updateTableStats);
        refreshInterval = STATS_SERVICE_REFRESH_INTERVAL_SETTING.setting().get(settings);
        refreshScheduledTask = scheduleRefresh(refreshInterval);
        sqlDirectExecutor = sqlOperations.createSystemExecutor("sys", TABLE_STATS, STMT, DEFAULT_SOFT_LIMIT);

        clusterService.getClusterSettings().addSettingsUpdateConsumer(
            STATS_SERVICE_REFRESH_INTERVAL_SETTING.setting(), this::setRefreshInterval);
    }

    @Override
    public void run() {
        updateStats();
    }

    private void updateStats() {
        if (clusterService.localNode() == null) {
            /*
              During a long startup (e.g. during an upgrade process) the localNode() may be null
              and this would lead to NullPointerException in the TransportExecutor.
             */
            logger.debug("Could not retrieve table stats. localNode is not fully available yet.");
            return;
        }

        try {
            sqlDirectExecutor.execute(resultReceiver, Collections.emptyList());
        } catch (Throwable t) {
            logger.error("error retrieving table stats", t);
        }
    }

    static class TableStatsResultReceiver extends BaseResultReceiver {

        private static final Logger LOGGER = Loggers.getLogger(TableStatsResultReceiver.class);

        private final Consumer<ObjectLongMap<TableIdent>> tableStatsConsumer;
        private ObjectLongMap<TableIdent> newStats = new ObjectLongHashMap<>();

        TableStatsResultReceiver(Consumer<ObjectLongMap<TableIdent>> tableStatsConsumer) {
            this.tableStatsConsumer = tableStatsConsumer;
        }

        @Override
        public void setNextRow(Row row) {
            TableIdent tableIdent = new TableIdent(BytesRefs.toString(row.get(1)), BytesRefs.toString(row.get(2)));
            newStats.put(tableIdent, ((long) row.get(0)));
        }

        @Override
        public void allFinished(boolean interrupted) {
            tableStatsConsumer.accept(newStats);
            newStats = new ObjectLongHashMap<>();
            super.allFinished(interrupted);
        }

        @Override
        public void fail(@Nonnull Throwable t) {
            LOGGER.error("error retrieving table stats", t);
            super.fail(t);
        }
    }

    private ThreadPool.Cancellable scheduleRefresh(TimeValue newRefreshInterval) {
        if (newRefreshInterval.millis() > 0) {
            return threadPool.scheduleWithFixedDelay(
                this,
                newRefreshInterval,
                ThreadPool.Names.REFRESH);
        }
        return null;
    }

    private void setRefreshInterval(TimeValue newRefreshInterval) {
        if (refreshScheduledTask != null) {
            refreshScheduledTask.cancel();
        }
        refreshScheduledTask = scheduleRefresh(newRefreshInterval);
        refreshInterval = newRefreshInterval;
    }
}

