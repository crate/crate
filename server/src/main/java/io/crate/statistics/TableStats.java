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

import io.crate.metadata.RelationName;
import io.crate.metadata.table.TableInfo;
import io.crate.statistics.arrow.ArrowBufferAllocator;
import io.crate.statistics.arrow.Statistics;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Holds table statistics that are updated periodically by {@link TableStatsService}.
 */
public class TableStats implements AutoCloseable {

    private volatile Map<RelationName, Statistics> tableStats = new HashMap<>();

    public void updateTableStats(Map<RelationName, Stats> tableStats) {
        HashMap<RelationName, Statistics> update = new HashMap<>();
        for (Map.Entry<RelationName, Stats> entry : tableStats.entrySet()) {
            RelationName relationName = entry.getKey();
            Stats stats = entry.getValue();
            Statistics statistics = new Statistics(
                ArrowBufferAllocator.INSTANCE,
                stats.numDocs,
                stats.sizeInBytes,
                stats.statsByColumn()
            );
            update.put(relationName, statistics);

        }
        this.tableStats = update;
    }

    /**
     * Returns the number of docs a table has.
     * <p>
     * <p>
     * The returned number isn't an accurate real-time value but a cached value that is periodically updated
     * </p>
     * Returns -1 if the table isn't in the cache
     */
    public long numDocs(RelationName relationName) {
        Statistics statistics = tableStats.get(relationName);
        if (statistics == null) {
            return Stats.EMPTY.numDocs();
        }
        return statistics.numDocs();
    }

    /**
     * Returns an estimation (avg) size of each row of the table in bytes.
     * <p>
     * <p>
     * The returned number isn't an accurate real-time value but a cached value that is periodically updated
     * </p>
     * Returns -1 if the table isn't in the cache
     */
    public long estimatedSizePerRow(RelationName relationName) {
        Statistics statistics = tableStats.get(relationName);
        if (statistics == null) {
            return Stats.EMPTY.averageSizePerRowInBytes();
        }
        return statistics.averageSizePerRowInBytes();
    }

    /**
     * Returns an estimation (avg) size of each row of the table in bytes or if no stats are available
     * for the given table an estimate (avg) based on the column types of the table.
     */
    public long estimatedSizePerRow(TableInfo tableInfo) {
        Statistics stats = tableStats.get(tableInfo.ident());
        if (stats == null) {
            // if stats are not available we fall back to estimate the size based on
            // column types. Therefore we need to get the column information.
            return Stats.EMPTY.estimateSizeForColumns(tableInfo);
        } else {
            return stats.averageSizePerRowInBytes();
        }
    }

    public Iterable<ColumnStatsEntry> statsEntries() {
        Set<Map.Entry<RelationName, Statistics>> entries = tableStats.entrySet();
        return () -> entries.stream()
            .flatMap(tableEntry -> {
                Statistics stats = tableEntry.getValue();
                return stats.statsByColumn().entrySet().stream()
                    .map(columnEntry ->
                        new ColumnStatsEntry(tableEntry.getKey(), columnEntry.getKey(), columnEntry.getValue()));
            }).iterator();
    }

    public Stats getStats(RelationName relationName) {
        Statistics statistics = tableStats.get(relationName);
        if (statistics == null) {
            return Stats.EMPTY;
        }
        return new Stats(statistics.numDocs(), statistics.sizeInBytes(), statistics.statsByColumn());
    }

    @Override
    public void close() throws Exception {
        for (Statistics stats : tableStats.values()) {
            stats.close();
        }
    }
}
