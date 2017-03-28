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

package io.crate.analyze;

import io.crate.action.sql.SessionContext;
import io.crate.planner.Plan;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.Statement;
import io.crate.testing.SQLExecutor;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.threadpool.TestThreadPool;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

@State(value = Scope.Benchmark)
public class PreExecutionBenchmark {

    private TestThreadPool threadPool;
    private SQLExecutor e;
    private Statement selectStatement;
    private Analysis selectAnalysis;
    private UUID jobId;

    @Setup
    public void setup() {
        threadPool = new TestThreadPool("testing");
        e = SQLExecutor.builder(
            new ClusterService(Settings.builder().put("cluster.name", "ClusterServiceTests").build(),
                new ClusterSettings(Settings.EMPTY, Sets.newHashSet(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)),
                threadPool)).
            enableDefaultTables().
            build();
        selectStatement = SqlParser.createStatement("select name from users");
        selectAnalysis =
            e.analyzer.boundAnalyze(selectStatement, SessionContext.SYSTEM_SESSION, ParameterContext.EMPTY);
        jobId = UUID.randomUUID();
    }

    @TearDown
    public void cleanup() throws InterruptedException {
        threadPool.shutdown();
        threadPool.awaitTermination(20, TimeUnit.SECONDS);
    }

    @Benchmark
    public Statement benchParse() throws Exception {
        return SqlParser.createStatement("select name from users");
    }

    @Benchmark
    public AnalyzedStatement benchParseAndAnalyzeSelect() {
        return e.analyze("select name from users");
    }

    @Benchmark
    public Plan benchParseAndAnalyzeAndPlan() {
        return e.plan("select name from users");
    }

    @Benchmark
    public Analysis benchAnalyze() {
        return e.analyzer.boundAnalyze(selectStatement, SessionContext.SYSTEM_SESSION, ParameterContext.EMPTY);
    }

    @Benchmark
    public Plan benchPlan() {
        return e.planner.plan(selectAnalysis, jobId, 0, 0);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
            .include(PreExecutionBenchmark.class.getSimpleName())
            .addProfiler(GCProfiler.class)
            .build();
        new Runner(opt).run();
    }
}
