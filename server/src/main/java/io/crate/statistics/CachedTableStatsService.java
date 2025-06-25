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

import java.util.Map;
import java.util.stream.Stream;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;


public class CachedTableStatsService implements TableStats {

    private final TableStatsPersistenceService tableStatsPersistenceService;

    private final Cache<RelationName, Stats> cache = Caffeine.newBuilder()
        .maximumSize(100)
        .build();

    public CachedTableStatsService(TableStatsPersistenceService tableStatsProvider) {
        this.tableStatsPersistenceService = tableStatsProvider;
    }

    public void updateTableStats(Map<RelationName, Stats> tableStats) {
        tableStatsPersistenceService.deleteTableStats();
        this.tableStatsPersistenceService.write(tableStats);
        cache.invalidateAll();
    }

    public void reset() {
        tableStatsPersistenceService.deleteTableStats();
        cache.invalidateAll();
    }


    public long numDocs(RelationName relationName) {
        return fromCache(relationName).numDocs();

    }

    public long estimatedSizePerRow(RelationName relationName) {
        return fromCache(relationName).averageSizePerRowInBytes();
    }

    private Stats fromCache(RelationName relationName) {
        Stats stats = cache.get(relationName, r -> tableStatsPersistenceService.apply(relationName));
        if (stats == null) {
            stats = Stats.EMPTY;
        }
        return stats;
    }

    public long estimatedSizePerRow(TableInfo tableInfo) {
        Stats stats = fromCache(tableInfo.ident());
        if (stats.isEmpty()) {
            // if stats are not available we fall back to estimate the size based on
            // column types. Therefore we need to get the column information.
            return stats.estimateSizeForColumns(tableInfo);
        } else {
            return stats.averageSizePerRowInBytes();
        }
    }

    public Iterable<ColumnStatsEntry> statsEntries(Iterable<RelationName> relationNames) {
        return () -> sequentialStream(relationNames)
            .flatMap(relationName -> {
                Stats stats = fromCache(relationName);
                if (stats.isEmpty()) {
                    return Stream.empty();
                } else {
                    return stats.statsByColumn().entrySet().stream()
                        .map(columnEntry ->
                            new ColumnStatsEntry(relationName, columnEntry.getKey(), columnEntry.getValue()));
                }
            }).iterator();
    }

    public Stats getStats(RelationName relationName) {
        return fromCache(relationName);
    }

}
