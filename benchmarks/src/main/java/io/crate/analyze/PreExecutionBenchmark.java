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

package io.crate.analyze;


import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.Node;
import org.elasticsearch.transport.Netty4Plugin;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import io.crate.action.sql.BaseResultReceiver;
import io.crate.action.sql.SQLOperations;
import io.crate.data.Row;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.NodeContext;
import io.crate.metadata.RoutingProvider;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.planner.PlannerContext;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.Statement;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(value = Scope.Benchmark)
public class PreExecutionBenchmark {

    private Analyzer analyzer;
    private Node node;
    private SQLOperations sqlOperations;
    private Planner planner;
    private ClusterService clusterService;
    private NodeContext nodeCtx;

    @Setup
    public void setup() throws Exception {
        Path tempDir = Files.createTempDirectory("");
        Settings settings = Settings.builder()
            .put("path.home", tempDir.toAbsolutePath().toString())
            .build();
        Environment environment = new Environment(settings, tempDir);
        node = new Node(
            environment,
            List.of(
                Netty4Plugin.class
            ),
            true
        );
        node.start();
        Injector injector = node.injector();
        sqlOperations = injector.getInstance(SQLOperations.class);
        analyzer = injector.getInstance(Analyzer.class);
        planner = injector.getInstance(Planner.class);
        clusterService = injector.getInstance(ClusterService.class);
        nodeCtx = injector.getInstance(NodeContext.class);

        String statement = "create table users (id int primary key, name string, date timestamp, text string index using fulltext)";
        var resultReceiver = new BaseResultReceiver();
        sqlOperations.newSystemSession()
            .quickExec(statement, resultReceiver, Row.EMPTY);
        resultReceiver.completionFuture().get(5, TimeUnit.SECONDS);
    }

    @TearDown
    public void teardown() throws Exception {
        node.close();
    }

    @Benchmark
    public Statement measure_parse_simple_select() throws Exception {
        return SqlParser.createStatement("select name from users");
    }

    @Benchmark
    public AnalyzedStatement measure_parse_and_analyze_simple_select() throws Exception {
        String sql = "select name from users";
        CoordinatorTxnCtx systemTransactionContext = CoordinatorTxnCtx.systemTransactionContext();
        Analysis analysis = new Analysis(systemTransactionContext, ParamTypeHints.EMPTY, null);
        return analyzer.analyzedStatement(SqlParser.createStatement(sql), analysis);
    }

    @Benchmark
    public Plan measure_parse_analyze_and_plan_simple_select() throws Exception {
        String sql = "select name from users";
        CoordinatorTxnCtx systemTransactionContext = CoordinatorTxnCtx.systemTransactionContext();
        Analysis analysis = new Analysis(systemTransactionContext, ParamTypeHints.EMPTY, null);
        AnalyzedStatement analyzedStatement = analyzer.analyzedStatement(SqlParser.createStatement(sql), analysis);
        var jobId = UUID.randomUUID();
        var routingProvider = new RoutingProvider(Randomness.get().nextInt(), planner.getAwarenessAttributes());
        var clusterState = clusterService.state();
        var txnCtx = CoordinatorTxnCtx.systemTransactionContext();
        var plannerContext = new PlannerContext(
            clusterState,
            routingProvider,
            jobId,
            txnCtx,
            nodeCtx,
            0,
            null);
        return planner.plan(analyzedStatement, plannerContext);
    }
}
