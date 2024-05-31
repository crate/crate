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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.common.inject.Singleton;

/**
 * Holds table statistics that are updated periodically by {@link TableStatsService}.
 */
@Singleton
public class TableStats {

    private volatile Map<RelationName, Stats> tableStats = new HashMap<>();

    public void updateTableStats(Map<RelationName, Stats> tableStats) {
        this.tableStats = tableStats;
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
        return tableStats.getOrDefault(relationName, Stats.EMPTY).numDocs;
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
        return tableStats.getOrDefault(relationName, Stats.EMPTY).averageSizePerRowInBytes();
    }

    public Iterable<ColumnStatsEntry> statsEntries() {
        Set<Map.Entry<RelationName, Stats>> entries = tableStats.entrySet();
        return () -> entries.stream()
            .flatMap(tableEntry -> {
                Stats stats = tableEntry.getValue();
                return stats.statsByColumn().entrySet().stream()
                    .map(columnEntry ->
                        new ColumnStatsEntry(tableEntry.getKey(), columnEntry.getKey(), columnEntry.getValue()));
            }).iterator();
    }

    public Stats getStats(RelationName relationName) {
        return tableStats.getOrDefault(relationName, Stats.EMPTY);
    }
}
