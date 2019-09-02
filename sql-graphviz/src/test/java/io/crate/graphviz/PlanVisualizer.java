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
import io.crate.planner.operators.NestedLoopJoin;
import io.crate.planner.operators.Order;
import io.crate.planner.operators.RelationBoundary;
import io.crate.testing.SQLExecutor;
import org.apache.logging.log4j.Level;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static io.crate.test.integration.ClusterServices.createClusterService;

public final class PlanVisualizer {

    public static void main(String[] args) throws Exception {
        final String loggerLevel = System.getProperty("es.logger.level", Level.ERROR.name());
        final Settings settings = Settings.builder().put("logger.level", loggerLevel).build();
        LogConfigurator.configureWithoutConfig(settings);
        System.setProperty("cratedb.skip_randomness_check", Boolean.TRUE.toString());
        var clusterService = createClusterService(
            List.of(),
            "PlanVisualizer",
            new ThreadPool(Settings.EMPTY)
        );
        if (args.length == 0) {
            String s = "select aa, xyi from (\n" +
                       "  select (xy + i) as xyi, aa from (\n" +
                       "    select concat(t1.a, t2.a) as aa, t2.i, (t1.x + t2.y) as xy \n" +
                       "    from t1, t2 where t1.a='a' or t2.a='aa') as t) as tt \n" +
                       "order by aa, xyi";
            args = new String[]{
                "create table t1 (a string, i integer, x integer)",
                "create table t2 (a string, i integer, y integer)",
                s
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
                    subQueryPlanner,
                    true,
                    e.functions(),
                    txnCtx,
                    Set.of(),
                    new TableStats()
                );
                try (var out = Files.newOutputStream(Paths.get("/tmp/plan.gv"))) {
                    out.write(generateDotOutput(plan).getBytes(StandardCharsets.UTF_8));
                }
                System.out.println(generateDotOutput(plan));
                LogicalPlan optimizedPlan = e.logicalPlanner.optimizer.optimize(plan);
                try (var out = Files.newOutputStream(Paths.get("/tmp/plan-optimized.gv"))) {
                    out.write(generateDotOutput(optimizedPlan).getBytes(StandardCharsets.UTF_8));
                }
                LogicalPlan optimizedWithFetch = optimizedPlan.rewriteForFetch(
                    FetchMode.MAYBE_RESET_USED_COLUMNS, new HashSet<>(normalizedRelation.outputs()), true);
                if (optimizedWithFetch != null) {
                    try (var out = Files.newOutputStream(Paths.get("/tmp/plan-optimized-fetch.gv"))) {
                        out.write(generateDotOutput(optimizedWithFetch).getBytes(StandardCharsets.UTF_8));
                    }
                }
                LogicalPlan planWithFetch = plan.rewriteForFetch(
                    FetchMode.MAYBE_RESET_USED_COLUMNS, new HashSet<>(normalizedRelation.outputs()), true);
                if (planWithFetch != null) {
                    try (var out = Files.newOutputStream(Paths.get("/tmp/plan-fetch.gv"))) {
                        out.write(generateDotOutput(planWithFetch).getBytes(StandardCharsets.UTF_8));
                    }
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
            context.sb.append(startTable("FetchOrEval(" + id + ")"));
            addRows(context.sb, fetchOrEval.outputs());
            endTable(context.sb);

            String sourceName = fetchOrEval.source().accept(this, context);

            context.sb.append(name);
            context.sb.append(" -> ");
            context.sb.append(sourceName);
            context.sb.append("\n");
            return name;
        }

        @Override
        public String visitCollect(Collect collect, Context context) {
            int id = context.idGen.incrementAndGet();
            String name = "\"Collect[" + id + "]\"";
            context.sb.append(name);
            context.sb.append(" [\n");
            context.sb.append(startTable("Collect(" + id + ")"));
            addRows(context.sb, collect.outputs());
            endTable(context.sb);

            return name;
        }

        @Override
        public String visitOrder(Order order, Context context) {
            int id = context.idGen.incrementAndGet();
            String name = "\"Order[" + id + "]\"";
            context.sb.append(name);
            context.sb.append(" [\n");

            context.sb.append(startTable("Order(" + id + ")"));
            addRows(context.sb, order.outputs());
            addRows(
                context.sb,
                order.orderBy().orderBySymbols().stream()
                    .map(x -> "ORDER BY " + x.toString())
                    .collect(Collectors.toList())
            );
            endTable(context.sb);

            String sourceName = order.source().accept(this, context);

            context.sb.append(name);
            context.sb.append(" -> ");
            context.sb.append(sourceName);
            context.sb.append("\n");
            return name;
        }

        @Override
        public String visitRelationBoundary(RelationBoundary boundary, Context context) {
            int id = context.idGen.incrementAndGet();
            String name = "\"RelationBoundary[" + id + "]\"";
            context.sb.append(name);
            context.sb.append(" [\n");

            context.sb.append(startTable("RelationBoundary(" + id + ")"));
            addRows(context.sb, boundary.outputs());
            context.sb.append("<TR><TD>---</TD></TR>\n");
            addRows(context.sb, boundary.reverseMapping().entrySet().stream()
                .map(e -> e.getKey().toString() + " → " + e.getValue().toString())
                .collect(Collectors.toList()));
            endTable(context.sb);

            String sourceName = boundary.source().accept(this, context);

            context.sb.append(name);
            context.sb.append(" -> ");
            context.sb.append(sourceName);
            context.sb.append("\n");
            return name;
        }

        @Override
        public String visitNestedLoopJoin(NestedLoopJoin nestedLoop, Context context) {
            int id = context.idGen.incrementAndGet();
            String name = "\"NestedLoop[" + id + "]\"";
            context.sb.append(name);
            context.sb.append(" [\n");

            context.sb.append(startTable("NestedLoop(" + id + "/" + nestedLoop.joinType() + "))"));
            Symbol joinCondition = nestedLoop.joinCondition();
            if (joinCondition != null) {
                addRow(context.sb, "JoinCondition: " + joinCondition.toString());
            }
            addRows(context.sb, nestedLoop.outputs());
            endTable(context.sb);

            for (LogicalPlan source : nestedLoop.sources()) {
                String sourceName = source.accept(this, context);
                context.sb.append(name);
                context.sb.append(" -> ");
                context.sb.append(sourceName);
                context.sb.append("\n");
            }
            return name;
        }

        private static void endTable(StringBuilder sb) {
            sb.append("</TABLE>>\n");
            sb.append("  shape=none\n");
            sb.append("]\n");
            sb.append("\n");
        }

        private static void addRows(StringBuilder sb, Iterable<?> items) {
            for (Object item : items) {
                addRow(sb, item);
            }
        }

        private static void addRow(StringBuilder sb, Object item) {
            sb.append("<TR><TD>")
                .append(item.toString())
                .append("</TD></TR>\n");
        }

        private static String startTable(String name) {
            return ("  label=<<TABLE CELLBORDER=\"0\">\n<TR><TD><B>" + name + "</B></TD></TR>\n");
        }

        @Override
        protected String visitPlan(LogicalPlan logicalPlan, Context context) {
            throw new UnsupportedOperationException("NYI: printing " + logicalPlan);
        }
    }
}
