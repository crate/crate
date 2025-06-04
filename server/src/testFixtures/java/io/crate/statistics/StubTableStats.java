/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import java.util.HashMap;
import java.util.Map;

import io.crate.metadata.RelationName;
import io.crate.metadata.table.TableInfo;

public class StubTableStats implements TableStats {

    private volatile Map<RelationName, Stats> tableStats = new HashMap<>();

    public void updateTableStats(Map<RelationName, Stats> tableStats) {
        this.tableStats = tableStats;
    }

    public void reset() {
        this.tableStats.clear();
    }

    public long numDocs(RelationName relationName) {
        return tableStats.getOrDefault(relationName, Stats.EMPTY).numDocs;
    }

    public long estimatedSizePerRow(RelationName relationName) {
        return tableStats.getOrDefault(relationName, Stats.EMPTY).averageSizePerRowInBytes();
    }

    public long estimatedSizePerRow(TableInfo tableInfo) {
        Stats stats = tableStats.get(tableInfo.ident());
        if (stats == null) {
            // if stats are not available we fall back to estimate the size based on
            // column types. Therefore we need to get the column information.
            return Stats.EMPTY.estimateSizeForColumns(tableInfo);
        } else {
            return stats.averageSizePerRowInBytes();
        }
    }


    public Iterable<ColumnStatsEntry> statsEntries(Iterable<RelationName> relationNames) {
        return () -> tableStats.entrySet().stream()
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
