/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.planner;

import com.google.common.base.Objects;
import com.google.common.base.Strings;
import io.crate.planner.node.PlanNode;
import io.crate.planner.node.PlanNodeVisitor;
import io.crate.planner.node.dql.*;
import io.crate.planner.node.dql.join.NestedLoopNode;
import io.crate.planner.projection.*;
import io.crate.planner.symbol.Aggregation;
import io.crate.planner.symbol.Symbol;
import io.crate.planner.symbol.SymbolVisitor;

import static java.lang.String.format;

public class PlanPrinter extends PlanVisitor<PlanPrinter.PrintContext, Void> {

    static class PrintContext {

        private int indent = 0;
        private final StringBuilder output;

        PrintContext(StringBuilder output) {
            this.output = output;
        }

        public int indent() {
            return ++indent;
        }

        public int dedent() {
            return --indent;
        }

        private void print(String format, Object... args) {
            String value;
            if (args.length == 0) {
                value = format;
            } else {
                value = format(format, args);
            }
            output.append(Strings.repeat("  ", indent)).append(value).append('\n');
        }
    }


    class ProjectionPrinter extends ProjectionVisitor<PrintContext, Void> {

        @Override
        protected Void visitProjection(Projection projection, PrintContext context) {
            context.print("Projection: %s", projection);
            return null;
        }

        @Override
        public Void visitAggregationProjection(AggregationProjection projection, PrintContext context) {
            context.print("AggregationProjection:");
            context.indent();
            context.print("aggregations:");
            context.indent();
            for (Aggregation aggregation : projection.aggregations()) {
                symbolPrinter.process(aggregation, context);
            }
            context.dedent();
            context.dedent();
            return null;
        }

        @Override
        public Void visitGroupProjection(GroupProjection projection, PrintContext context) {
            context.print("GroupProjection:");
            context.indent();
            context.print("group by keys:");
            context.indent();
            for (Symbol key : projection.keys()) {
                symbolPrinter.process(key, context);
            }
            context.dedent();
            context.print("aggregations:");
            context.indent();
            for (Aggregation aggregation : projection.values()) {
                symbolPrinter.process(aggregation, context);
            }
            context.dedent();
            context.print("outputs:");
            context.indent();
            for (Symbol output : projection.outputs()) {
                symbolPrinter.process(output, context);
            }
            context.dedent();
            context.dedent();
            return null;
        }

        @Override
        public Void visitFilterProjection(FilterProjection projection, PrintContext context) {
            context.print("FilterProjection:");
            context.indent();

            context.print("having clause:");
            symbolPrinter.process(projection.query(), context);

            context.print("outputs:");
            context.indent();
            for (Symbol output : projection.outputs()) {
                symbolPrinter.process(output, context);
            }
            context.dedent();

            context.dedent();
            return null;
        }
    }

    static class SymbolPrinter extends SymbolVisitor<PrintContext, Void> {

        @Override
        protected Void visitSymbol(Symbol symbol, PrintContext context) {
            context.print("Symbol: %s", symbol);
            return null;
        }


        @Override
        public Void visitAggregation(Aggregation symbol, PrintContext context) {
            context.print(Objects.toStringHelper(symbol)
                    .add("functionIdent", symbol.functionIdent())
                    .add("inputs", symbol.inputs())
                    .add("fromStep", symbol.fromStep())
                    .add("toStep", symbol.toStep())
                    .toString());
            return null;
        }
    }


    class PlanNodePrinter extends PlanNodeVisitor<PlanPrinter.PrintContext, Void> {

        @Override
        public Void visitMergeNode(MergeNode node, PrintContext context) {
            context.print("Merge");
            context.indent();
            context.print("executionNodes: %s", node.executionNodes());
            processProjections(node, context);
            context.dedent();
            return null;
        }

        @Override
        public Void visitESGetNode(ESGetNode node, PrintContext context) {
            context.print(node.toString());
            context.indent();
            context.print("outputs:");
            for (Symbol symbol : node.outputs()) {
                symbolPrinter.process(symbol, context);
            }
            context.dedent();
            return null;
        }

        @Override
        public Void visitQueryThenFetchNode(QueryThenFetchNode node, PrintContext context) {
            context.print(node.toString());
            context.indent();
            context.print("outputs:");
            for (Symbol symbol : node.outputs()) {
                symbolPrinter.process(symbol, context);
            }

            context.dedent();
            return null;
        }

        @Override
        public Void visitCollectNode(CollectNode node, PrintContext context) {
            context.print("Collect");
            context.indent();
            context.print("routing: %s", node.routing());
            context.print("toCollect:");
            for (Symbol symbol : node.toCollect()) {
                symbolPrinter.process(symbol, context);
            }
            context.print("whereClause %s", node.whereClause().toString());

            processProjections(node, context);
            context.dedent();

            return null;
        }


        public Void visitNestedLoopNode(NestedLoopNode node, PrintContext context) {
            context.print(node.getClass().getSimpleName());
            context.indent();
            context.print("limit: %s", node.limit());
            context.print("offset: %s", node.offset());

            context.print("left");
            context.indent();
            planNodePrinter.process(node.left(), context);
            context.dedent();

            context.print("right");
            context.indent();
            planNodePrinter.process(node.right(), context);
            context.dedent();

            context.print("outputTypes: %s", node.outputTypes());

            context.print("projections: ");
            for (Projection projection : node.projections()) {
                projectionPrinter.process(projection, context);
            }
            context.dedent();
            return null;
        }
    }

    private ProjectionPrinter projectionPrinter;
    private SymbolPrinter symbolPrinter;
    private final PlanNodePrinter planNodePrinter;



    public PlanPrinter() {
        projectionPrinter = new ProjectionPrinter();
        symbolPrinter = new SymbolPrinter();
        planNodePrinter = new PlanNodePrinter();
    }

    public String print(Plan plan) {
        StringBuilder output = new StringBuilder();
        PrintContext context = new PrintContext(output);
        process(plan, context);
        return output.toString();
    }


    private void processProjections(DQLPlanNode node, PrintContext context) {
        if (node.hasProjections()) {
            context.print(node.toString());
            context.indent();
            context.print("projections: ");
            for (Projection projection : node.projections()) {
                projectionPrinter.process(projection, context);
            }
            context.dedent();
        }
    }

    @Override
    protected Void visitPlan(Plan plan, PrintContext context) {
        context.print("Plan: " + plan.getClass().getCanonicalName());
        return null;
    }

    @Override
    public Void visitIterablePlan(IterablePlan plan, PrintContext context) {
        visitPlan(plan, context);
        context.indent();
        for (PlanNode node : plan) {
            planNodePrinter.process(node, context);
        }
        context.dedent();
        return null;
    }
}
