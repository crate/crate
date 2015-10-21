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


import com.carrotsearch.hppc.ObjectLongMap;
import com.carrotsearch.hppc.ObjectLongOpenHashMap;
import io.crate.action.sql.SQLRequest;
import io.crate.action.sql.SQLResponse;
import io.crate.action.sql.TransportSQLAction;
import io.crate.metadata.TableIdent;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.BindingAnnotation;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.concurrent.TimeUnit;

@Singleton
public class TableStatsService extends AbstractComponent implements Runnable {

    private static final SQLRequest REQUEST = new SQLRequest(
            "select cast(sum(num_docs) as long), schema_name, table_name from sys.shards group by 2, 3");
    private final Provider<TransportSQLAction> transportSQLAction;
    private volatile ObjectLongMap<TableIdent> tableStats = null;

    @BindingAnnotation
    @Target({ ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD })
    @Retention(RetentionPolicy.RUNTIME)
    public @interface StatsUpdateInterval {}

    @Inject
    public TableStatsService(Settings settings,
                             ThreadPool threadPool,
                             @StatsUpdateInterval TimeValue updateInterval,
                             Provider<TransportSQLAction> transportSQLAction) {
        super(settings);
        this.transportSQLAction = transportSQLAction;
        threadPool.scheduleWithFixedDelay(this, updateInterval);
    }

    @Override
    public void run() {
        updateStats();
    }

    private void updateStats() {
        transportSQLAction.get().execute(
                REQUEST,
                new ActionListener<SQLResponse>() {

                    @Override
                    public void onResponse(SQLResponse sqlResponse) {
                        tableStats = statsFromResponse(sqlResponse);
                    }

                    @Override
                    public void onFailure(Throwable e) {
                        logger.error("error retrieving table stats", e);
                    }
                });
    }

    private static ObjectLongMap<TableIdent> statsFromResponse(SQLResponse sqlResponse) {
        ObjectLongMap<TableIdent> newStats = new ObjectLongOpenHashMap<>((int) sqlResponse.rowCount());
        for (Object[] row : sqlResponse.rows()) {
            newStats.put(new TableIdent((String) row[1], (String) row[2]), (long) row[0]);
        }
        return newStats;
    }

    /**
     * Returns the number of docs a table has.
     *
     * <p>
     * The returned number isn't an accurate real-time value but a cached value that is periodically updated
     * </p>
     * Returns -1 if the table isn't in the cache
     */
    public long numDocs(TableIdent tableIdent) {
        ObjectLongMap<TableIdent> stats = tableStats;
        if (stats == null) {
            stats = statsFromResponse(transportSQLAction.get().execute(REQUEST).actionGet(30, TimeUnit.SECONDS));
            tableStats = stats;
        }
        if (stats.containsKey(tableIdent)) {
            return stats.get(tableIdent);
        }
        return -1;
    }
}
