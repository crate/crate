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

package io.crate.graphviz;

import io.crate.action.sql.SessionContext;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.planner.PlannerContext;
import io.crate.planner.SubqueryPlanner;
import io.crate.planner.TableStats;
import io.crate.planner.consumer.FetchMode;
import io.crate.planner.operators.Collect;
import io.crate.planner.operators.FetchOrEval;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.operators.LogicalPlanVisitor;
import io.crate.planner.operators.LogicalPlanner;
import io.crate.testing.SQLExecutor;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.config.Configurator;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static io.crate.test.integration.ClusterServices.createClusterService;

public final class PlanVisualizer {

    public static void main(String[] args) throws Exception {
        final String loggerLevel = System.getProperty("es.logger.level", Level.ERROR.name());
        final Settings settings = Settings.builder().put("logger.level", loggerLevel).build();
        LogConfigurator.configureWithoutConfig(settings);
        var clusterService = createClusterService(
            List.of(),
            "PlanVisualizer",
            new ThreadPool(Settings.EMPTY)
        );
        if (args.length == 0) {
            args = new String[]{
                "create table t1 (x int)",
                "select * from t1"
            };
        }
        SQLExecutor.Builder builder = SQLExecutor.builder(clusterService, 2, new Random());
        for (String arg : args) {
            if (arg.toLowerCase(Locale.ENGLISH).startsWith("create table")) {
                builder.addTable(arg);
            }
        }
        SQLExecutor e = builder.build();
        for (String arg : args) {
            if (arg.toLowerCase(Locale.ENGLISH).startsWith("select")) {
                SessionContext sessionContext = e.getSessionContext();
                CoordinatorTxnCtx txnCtx = new CoordinatorTxnCtx(sessionContext);
                PlannerContext plannerContext = e.getPlannerContext(clusterService.state());
                AnalyzedRelation relation = e.analyze(arg);
                AnalyzedRelation normalizedRelation = e.logicalPlanner.relationNormalizer.normalize(relation, txnCtx);

                var subQueryPlanner = new SubqueryPlanner(s -> e.logicalPlanner.planSubSelect(s, plannerContext));
                LogicalPlan plan = LogicalPlanner.plan(
                    normalizedRelation,
                    FetchMode.MAYBE_CLEAR,
                    subQueryPlanner,
                    true,
                    e.functions(),
                    txnCtx).build(new TableStats(), Set.of(), new HashSet<>(normalizedRelation.outputs()));
                try (var out = Files.newOutputStream(Paths.get("/tmp/plan-1.gv"))) {
                    out.write(generateDotOutput(plan).getBytes(StandardCharsets.UTF_8));
                }
                System.out.println(generateDotOutput(plan));
                LogicalPlan optimizedPlan = e.logicalPlanner.optimizer.optimize(plan);
                try (var out = Files.newOutputStream(Paths.get("/tmp/plan-2.gv"))) {
                    out.write(generateDotOutput(optimizedPlan).getBytes(StandardCharsets.UTF_8));
                }
                System.out.println();
                System.out.println();
                //System.out.println(generateDotOutput(optimizedPlan));
            }
        }
    }

    private static String generateDotOutput(LogicalPlan plan) {
        Context context = new Context();
        context.sb.append("digraph G {\n");
        context.sb.append("  graph [layout=dot]\n");
        plan.accept(NodePrinter.INSTANCE, context);
        context.sb.append("\n}");
        return context.sb.toString();
    }

    private static class Context {

        private final StringBuilder sb;
        private final AtomicInteger idGen;

        Context() {
            sb = new StringBuilder();
            idGen = new AtomicInteger();
        }
    }

    private static class NodePrinter extends LogicalPlanVisitor<Context, String> {

        static final NodePrinter INSTANCE = new NodePrinter();

        @Override
        public String visitFetchOrEval(FetchOrEval fetchOrEval, Context context) {
            int id = context.idGen.incrementAndGet();
            String name = "\"FetchOrEval[" + id + "]\"";
            context.sb.append(name);
            context.sb.append(" [\n");

            context.sb.append("  label=\"FetchOrEval(").append(id).append(")");
            for (Symbol output : fetchOrEval.outputs()) {
                context.sb.append("| ");
                context.sb.append(output.toString().replace("{", "\\{").replace("}", "\\}"));
            }
            context.sb.append("\"\n");
            context.sb.append("  shape=record\n");
            context.sb.append("]\n");
            context.sb.append("\n");

            String sourceName = fetchOrEval.source().accept(this, context);

            context.sb.append(name);
            context.sb.append(" -> ");
            context.sb.append(sourceName);
            return name;
        }

        @Override
        public String visitCollect(Collect collect, Context context) {
            int id = context.idGen.incrementAndGet();
            String name = "\"Collect[" + id + "]\"";
            context.sb.append(name);
            context.sb.append(" [\n");

            context.sb.append("  label=\"Collect(").append(id).append(")");
            for (Symbol output : collect.outputs()) {
                context.sb.append("| ");
                context.sb.append(output.toString().replace("{", "\\{").replace("}", "\\}"));
            }
            context.sb.append("\"\n");
            context.sb.append("  shape=record\n");
            context.sb.append("]\n");
            context.sb.append("\n");

            return name;
        }
    }
}
