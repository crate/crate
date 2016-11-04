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
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.threadpool.TestThreadPool;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.UUID;

@State(value = Scope.Benchmark)
public class PreExecutionBenchmark {

    // TODO: need to close threadPool
    private SQLExecutor e = SQLExecutor.builder(
        ClusterServiceUtils.createClusterService(new TestThreadPool("benchmarks"))
    ).enableDefaultTables().build();
    private Statement selectStatement = SqlParser.createStatement("select name from users");
    private Analysis selectAnalysis = e.analyzer.boundAnalyze(selectStatement, SessionContext.SYSTEM_SESSION, ParameterContext.EMPTY);
    private UUID jobId = UUID.randomUUID();

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
