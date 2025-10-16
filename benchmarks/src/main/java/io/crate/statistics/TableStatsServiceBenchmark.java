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

import static io.crate.statistics.TableStatsService.STATS_SERVICE_REFRESH_INTERVAL_SETTING;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

import io.crate.common.unit.TimeValue;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.session.Sessions;
import io.crate.types.DataTypes;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class TableStatsServiceBenchmark {

    private TableStatsService tableStatsService;
    private Map<RelationName, Stats> stats;

    @Setup
    public void setup() throws IOException {
        Random random = new Random(System.currentTimeMillis());
        var clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(new ClusterSettings(
            Settings.builder().put(STATS_SERVICE_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueHours(24)).build(),
            Set.of(STATS_SERVICE_REFRESH_INTERVAL_SETTING)));
        tableStatsService = new TableStatsService(
            Settings.builder().put(STATS_SERVICE_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueHours(24)).build(),
            new ThreadPool(Settings.EMPTY),
            clusterService,
            mock(Sessions.class),
            Files.createTempDirectory("bench_table_stats")
        );
        stats = HashMap.newHashMap(500);
        for (int i = 0; i < 500; i++) {
            var relationName = new RelationName("schema_" + i, "table" + i);
            long numDocs = i * 1000;
            long sizeInBytes = i * 10000;
            Map<ColumnIdent, ColumnStats<?>> colStats = HashMap.newHashMap(1000);
            for (int j = 0; j < 1000; j++) {
                var colIdent = ColumnIdent.of("col_" + i);
                List<Long> mcvList = new ArrayList<>(100);
                double[] mvcFreq = new double[100];
                List<Long> hist = new ArrayList<>(100);

                for (int k = 0; k < 100; k++) {
                    mcvList.add(random.nextLong());
                    mvcFreq[k] = random.nextDouble();
                    hist.add(random.nextLong());
                }

                colStats.put(
                    colIdent,
                    new ColumnStats<>(
                        random.nextDouble(),
                        random.nextDouble(),
                        random.nextDouble(),
                        DataTypes.LONG,
                        new MostCommonValues<>(mcvList, mvcFreq),
                        hist)
                );
            }
            stats.put(relationName, new Stats(numDocs, sizeInBytes, colStats));
        }
        tableStatsService.update(stats);
    }

    @Benchmark
    public void measureLoadColStatsFromDisk(Blackhole blackhole) {
        for (int i = 0; i < 500; i+=5) {
            var relationName = new RelationName("schema_" + i, "table" + i);
            for (int j = 0; j < 1000; j+=9) {
                var colIdent = ColumnIdent.of("col_" + i);
                blackhole.consume(tableStatsService.loadColStatsFromDisk(relationName, colIdent));
            }
        }
    }
}
