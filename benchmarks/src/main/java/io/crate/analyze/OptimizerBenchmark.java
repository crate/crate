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
import io.crate.action.sql.Cursors;
import io.crate.action.sql.Sessions;
import io.crate.data.Row;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.NodeContext;
import io.crate.metadata.RoutingProvider;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.planner.PlannerContext;
import io.crate.planner.operators.LogicalPlanner;
import io.crate.protocols.postgres.TransactionState;
import io.crate.sql.parser.SqlParser;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(value = Scope.Benchmark)
public class OptimizerBenchmark {

    private Analyzer analyzer;
    private Node node;
    private Sessions sqlOperations;
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
        sqlOperations = injector.getInstance(Sessions.class);
        analyzer = injector.getInstance(Analyzer.class);
        planner = injector.getInstance(Planner.class);
        clusterService = injector.getInstance(ClusterService.class);
        nodeCtx = injector.getInstance(NodeContext.class);

        var resultReceiver = new BaseResultReceiver();
        sqlOperations.newSystemSession().quickExec(T1_DEFINITION, resultReceiver, Row.EMPTY);
        resultReceiver.completionFuture().get(5, TimeUnit.SECONDS);

        resultReceiver = new BaseResultReceiver();
        sqlOperations.newSystemSession().quickExec(T2_DEFINITION, resultReceiver, Row.EMPTY);
        resultReceiver.completionFuture().get(5, TimeUnit.SECONDS);

        resultReceiver = new BaseResultReceiver();
        sqlOperations.newSystemSession().quickExec(T3_DEFINITION, resultReceiver, Row.EMPTY);
        resultReceiver.completionFuture().get(5, TimeUnit.SECONDS);
    }

    public static final String T1_DEFINITION =
        "create table doc.t1 (" +
        "  a text," +
        "  x int," +
        "  i int" +
        ")";

    public static final String T2_DEFINITION =
        "create table doc.t2 (" +
        "  b text," +
        "  y int," +
        "  i int" +
        ")";

    public static final String T3_DEFINITION =
        "create table doc.t3 (" +
        "  c text," +
        "  z int" +
        ")";

    @TearDown
    public void teardown() throws Exception {
        node.close();
    }
    final String stmt = """
            SELECT
                columns.table_name,
                columns.column_name
            FROM
                information_schema.columns
                LEFT JOIN pg_catalog.pg_attribute AS col_attr
                    ON col_attr.attname = columns.column_name
                    AND col_attr.attrelid = (
                        SELECT
                            pg_class.oid
                        FROM
                            pg_catalog.pg_class
                            LEFT JOIN pg_catalog.pg_namespace ON pg_namespace.oid = pg_class.relnamespace
                        WHERE
                            pg_class.relname = columns.table_name
                            AND pg_namespace.nspname = columns.table_schema
                    )
                    AND col_attr.attrelid = (SELECT col_attr.attrelid)

            ORDER BY 1, 2 DESC
            LIMIT 3
            """;

    @Benchmark
    public Plan plan_rule_optimizer() throws Exception {
        LogicalPlanner.useIterativeOptimizer = false;
        CoordinatorTxnCtx systemTransactionContext = CoordinatorTxnCtx.systemTransactionContext();
        Analysis analysis = new Analysis(systemTransactionContext, ParamTypeHints.EMPTY, Cursors.EMPTY);
        AnalyzedStatement analyzedStatement = analyzer.analyzedStatement(SqlParser.createStatement(stmt), analysis);
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
            null,
            Cursors.EMPTY,
            TransactionState.IDLE
        );
        return planner.plan(analyzedStatement, plannerContext);
    }

    @Benchmark
    public Plan plan_iterative_optimizer() throws Exception {
        LogicalPlanner.useIterativeOptimizer = true;
        CoordinatorTxnCtx systemTransactionContext = CoordinatorTxnCtx.systemTransactionContext();
        Analysis analysis = new Analysis(systemTransactionContext, ParamTypeHints.EMPTY, Cursors.EMPTY);
        AnalyzedStatement analyzedStatement = analyzer.analyzedStatement(SqlParser.createStatement(stmt), analysis);
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
            null,
            Cursors.EMPTY,
            TransactionState.IDLE
        );
        return planner.plan(analyzedStatement, plannerContext);
    }


}
