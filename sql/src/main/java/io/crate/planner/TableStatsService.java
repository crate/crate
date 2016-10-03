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
import io.crate.action.sql.BaseResultReceiver;
import io.crate.action.sql.Option;
import io.crate.action.sql.SQLOperations;
import io.crate.core.collections.Row;
import io.crate.metadata.TableIdent;
import io.crate.types.DataType;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.BindingAnnotation;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;

import javax.annotation.Nonnull;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Singleton
public class TableStatsService extends AbstractComponent implements Runnable {

    static final String UNNAMED = "";
    static final int DEFAULT_SOFT_LIMIT = 10_000;
    static final String STMT =
        "select cast(sum(num_docs) as long), schema_name, table_name from sys.shards group by 2, 3";

    private final ClusterService clusterService;
    private final Provider<SQLOperations> sqlOperationsProvider;
    private volatile ObjectLongMap<TableIdent> tableStats = null;

    @BindingAnnotation
    @Target({ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD})
    @Retention(RetentionPolicy.RUNTIME)
    public @interface StatsUpdateInterval {
    }


    @Inject
    public TableStatsService(Settings settings,
                             ThreadPool threadPool,
                             ClusterService clusterService,
                             @StatsUpdateInterval TimeValue updateInterval,
                             Provider<SQLOperations> sqlOperationsProvider) {
        super(settings);
        this.clusterService = clusterService;
        this.sqlOperationsProvider = sqlOperationsProvider;
        threadPool.scheduleWithFixedDelay(this, updateInterval);
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

        SQLOperations.Session session =
            sqlOperationsProvider.get().createSession("sys", Option.NONE, DEFAULT_SOFT_LIMIT);
        try {
            session.parse(UNNAMED, STMT, Collections.<DataType>emptyList());
            session.bind(UNNAMED, UNNAMED, Collections.emptyList(), null);
            session.execute(UNNAMED, 0, new TableStatsResultReceiver());
            session.sync();
        } catch (Throwable t) {
            logger.error("error retrieving table stats", t);
        }
    }

    class TableStatsResultReceiver extends BaseResultReceiver {

        private final List<Object[]> rows = new ArrayList<>();

        @Override
        public void setNextRow(Row row) {
            rows.add(row.materialize());
        }

        @Override
        public void allFinished() {
            tableStats = statsFromRows(rows);
            super.allFinished();
        }

        @Override
        public void fail(@Nonnull Throwable t) {
            logger.error("error retrieving table stats", t);
            super.fail(t);
        }
    }

    ObjectLongMap<TableIdent> statsFromRows(List<Object[]> rows) {
        ObjectLongMap<TableIdent> newStats = new ObjectLongHashMap<>(rows.size());
        for (Object[] row : rows) {
            newStats.put(new TableIdent(BytesRefs.toString(row[1]), BytesRefs.toString(row[2])), (long) row[0]);
        }
        return newStats;
    }

    /**
     * Returns the number of docs a table has.
     * <p>
     * <p>
     * The returned number isn't an accurate real-time value but a cached value that is periodically updated
     * </p>
     * Returns -1 if the table isn't in the cache
     */
    public long numDocs(TableIdent tableIdent) {
        ObjectLongMap<TableIdent> stats = tableStats;
        if (stats != null && stats.containsKey(tableIdent)) {
            return stats.get(tableIdent);
        }
        return -1;
    }
}

