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

import static io.crate.common.collections.Iterables.sequentialStream;

import io.crate.metadata.RelationName;
import io.crate.metadata.table.TableInfo;

import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Holds table statistics that are updated periodically by {@link TableStatsService}.
 */
public class TableStats {

    private volatile Function<RelationName, Stats> tableStats = x -> null;

    public void updateTableStats(Function<RelationName, Stats> tableStats) {
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
        return getStats(relationName).numDocs();
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
        return getStats(relationName).averageSizePerRowInBytes();
    }

    /**
     * Returns an estimation (avg) size of each row of the table in bytes or if no stats are available
     * for the given table an estimate (avg) based on the column types of the table.
     */
    public long estimatedSizePerRow(TableInfo tableInfo) {
        Stats stats = tableStats.apply(tableInfo.ident());
        if (stats == null) {
            // if stats are not available we fall back to estimate the size based on
            // column types. Therefore we need to get the column information.
            return Stats.EMPTY.estimateSizeForColumns(tableInfo);
        } else {
            return stats.averageSizePerRowInBytes();
        }
    }

    public Iterable<ColumnStatsEntry> statsEntries(Iterable<RelationName> relationNames) {
        return () -> sequentialStream(relationNames)
            .flatMap(relationName -> {
                Stats stats = getStats(relationName);
                if (stats == Stats.EMPTY) {
                    return Stream.empty();
                } else {
                    return stats.statsByColumn().entrySet().stream()
                        .map(columnEntry ->
                            new ColumnStatsEntry(relationName, columnEntry.getKey(), columnEntry.getValue()));
                }
            }).iterator();
    }

    public Stats getStats(RelationName relationName) {
        Stats stats = tableStats.apply(relationName);
        return stats == null ? Stats.EMPTY : stats;
    }
}
