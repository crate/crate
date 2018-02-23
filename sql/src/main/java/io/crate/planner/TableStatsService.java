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


import com.carrotsearch.hppc.ObjectObjectHashMap;
import com.carrotsearch.hppc.ObjectObjectMap;
import com.google.common.annotations.VisibleForTesting;
import io.crate.action.sql.BaseResultReceiver;
import io.crate.action.sql.SQLOperations;
import io.crate.action.sql.Session;
import io.crate.data.Row;
import io.crate.metadata.TableIdent;
import io.crate.settings.CrateSetting;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.Statement;
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
import java.util.function.Consumer;

/**
 * Periodically refresh {@link TableStats} based on {@link #refreshInterval}.
 */
@Singleton
public class TableStatsService extends AbstractComponent implements Runnable {

    public static final CrateSetting<TimeValue> STATS_SERVICE_REFRESH_INTERVAL_SETTING = CrateSetting.of(Setting.timeSetting(
        "stats.service.interval", TimeValue.timeValueHours(1), Setting.Property.NodeScope, Setting.Property.Dynamic),
        DataTypes.STRING);

    static final String STMT = "select cast(sum(num_docs) as long), cast(sum(size) as long), schema_name, table_name " +
                               "from sys.shards where primary=true group by 3, 4";
    private static final Statement PARSED_STMT = SqlParser.createStatement(STMT);

    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final TableStatsResultReceiver resultReceiver;
    private final Session session;

    @VisibleForTesting
    ThreadPool.Cancellable refreshScheduledTask;
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
        session = sqlOperations.newSystemSession();

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
            session.quickExec(STMT, stmt -> PARSED_STMT, resultReceiver, Row.EMPTY);
        } catch (Throwable t) {
            logger.error("error retrieving table stats", t);
        }
    }

    static class TableStatsResultReceiver extends BaseResultReceiver {

        private static final Logger LOGGER = Loggers.getLogger(TableStatsResultReceiver.class);

        private final Consumer<ObjectObjectMap<TableIdent, TableStats.Stats>> tableStatsConsumer;
        private ObjectObjectMap<TableIdent, TableStats.Stats> newStats = new ObjectObjectHashMap<>();

        TableStatsResultReceiver(Consumer<ObjectObjectMap<TableIdent, TableStats.Stats>> tableStatsConsumer) {
            this.tableStatsConsumer = tableStatsConsumer;
        }

        @Override
        public void setNextRow(Row row) {
            TableIdent tableIdent = new TableIdent(BytesRefs.toString(row.get(2)), BytesRefs.toString(row.get(3)));
            newStats.put(tableIdent, new TableStats.Stats((long) row.get(0), (long) row.get(1)));
        }

        @Override
        public void allFinished(boolean interrupted) {
            tableStatsConsumer.accept(newStats);
            newStats = new ObjectObjectHashMap<>();
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

