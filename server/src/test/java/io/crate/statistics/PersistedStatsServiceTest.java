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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.types.DataTypes;

public class PersistedStatsServiceTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void testPersistStats() throws IOException {
        ColumnStats<Integer> columnStats = StatsUtils.statsFromValues(
            DataTypes.INTEGER, List.of(1, 2, 3, 4, 5, 6, 7, 8, 9)
        );

        Stats stats = new Stats(9L, 9L * DataTypes.INTEGER.fixedSize(), Map.of(
            ColumnIdent.of("x"), columnStats,
            ColumnIdent.of("y"), columnStats)
        );
        RelationName relationName = RelationName.fromIndexName("doc.test");
        Path dataPath = createDataPath();

        PersistedStatsService statsService = new PersistedStatsService(dataPath);
        statsService.add(relationName, stats);
        Stats loaded = statsService.get(relationName);
        assertThat(loaded).isEqualTo(stats);
    }

    @Test
    public void testClear() throws IOException {
        ColumnStats<Integer> columnStats = StatsUtils.statsFromValues(
            DataTypes.INTEGER, List.of(1, 2, 3, 4, 5, 6, 7, 8, 9)
        );

        Stats stats = new Stats(9L, 9L * DataTypes.INTEGER.fixedSize(), Map.of(
            ColumnIdent.of("x"), columnStats,
            ColumnIdent.of("y"), columnStats)
        );
        RelationName relationName = RelationName.fromIndexName("doc.test");
        Path dataPath = createDataPath();

        PersistedStatsService statsService = new PersistedStatsService(dataPath);
        statsService.add(relationName, stats);
        statsService.clear();;
        Stats loaded = statsService.get(relationName);
        assertThat(loaded).isNull();;
    }

    @Test
    public void testPersistMultipleStats() throws IOException {
        ColumnStats<Integer> columnStats = StatsUtils.statsFromValues(
            DataTypes.INTEGER, List.of(1, 2, 3, 4, 5, 6, 7, 8, 9)
        );

        Stats stats1 = new Stats(9L, 9L * DataTypes.INTEGER.fixedSize(), Map.of(
            ColumnIdent.of("a"), columnStats,
            ColumnIdent.of("b"), columnStats)
        );

        Stats stats2 = new Stats(9L, 9L * DataTypes.INTEGER.fixedSize(), Map.of(
            ColumnIdent.of("c"), columnStats,
            ColumnIdent.of("d"), columnStats)
        );

        RelationName table1 = RelationName.fromIndexName("doc.test1");
        RelationName table2 = RelationName.fromIndexName("doc.test2");

        Map<RelationName, Stats> tableStats = Map.of(
            table1, stats1,
            table2, stats2
        );

        Path dataPath = createDataPath();

        PersistedStatsService statsService = new PersistedStatsService(dataPath);
        statsService.add(tableStats);
        assertThat(statsService.get(table1)).isEqualTo(stats1);
        assertThat(statsService.get(table2)).isEqualTo(stats2);
    }

    public static Path createDataPath() {
        return createTempDir();
    }

}
