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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.crate.analyze.Analysis;
import io.crate.planner.node.CollectNode;
import io.crate.planner.node.ESSearchNode;
import io.crate.planner.node.MergeNode;
import io.crate.planner.projection.AggregationProjection;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.TopNProjection;
import io.crate.planner.symbol.*;
import io.crate.sql.tree.DefaultTraversalVisitor;
import org.elasticsearch.common.inject.Singleton;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Singleton
public class Planner extends DefaultTraversalVisitor<Symbol, Analysis> {


    static class Context {

        private Map<Symbol, InputColumn> symbols = new LinkedHashMap<>();
        private Map<Aggregation, InputColumn> aggregations = new LinkedHashMap<>();
        private Aggregation.Step toAggStep;
        private boolean grouped;

        Context(Aggregation.Step toAggStep, boolean grouped) {
            this.toAggStep = toAggStep;
            this.grouped = grouped;
        }

        public InputColumn allocateSymbol(Symbol symbol) {
            InputColumn ic = symbols.get(symbol);
            if (ic == null) {
                ic = new InputColumn(symbols.size());
                symbols.put(symbol, ic);
            }
            return ic;
        }


        public Symbol allocateAggregation(Aggregation aggregation) {
            InputColumn ic = aggregations.get(aggregation);
            if (ic == null) {
                ic = new InputColumn(aggregations.size());
                aggregations.put(aggregation, ic);
            }
            return ic;

        }

        public Symbol allocateAggregation(Function function) {
            if (toAggStep == null) {
                return function;
            }
            Aggregation agg = new Aggregation(function.info().ident(), function.arguments(),
                    Aggregation.Step.ITER, toAggStep);

            Symbol ic = allocateAggregation(agg);
            // split the aggregation if we are not final
            if (toAggStep == Aggregation.Step.FINAL) {
                return ic;
            } else {
                return new Aggregation(function.info().ident(), ImmutableList.<Symbol>of(ic),
                        toAggStep, Aggregation.Step.FINAL);
            }
        }

        public List<Symbol> symbolList() {
            List<Symbol> result = new ArrayList<>(symbols.size());
            for (Symbol symbol : symbols.keySet()) {
                result.add(symbol);
            }
            return result;
        }

        public List<Aggregation> aggregationList() {
            List<Aggregation> result = new ArrayList<>(aggregations.size());
            for (Aggregation symbol : aggregations.keySet()) {
                result.add(symbol);
            }
            return result;
        }


    }

    static class SplittingPlanNodeVisitor extends SymbolVisitor<Context, Symbol> {

        @Override
        protected Symbol visitSymbol(Symbol symbol, Context context) {
            return context.allocateSymbol(symbol);
        }

        @Override
        public Symbol visitInputColumn(InputColumn inputColumn, Context context) {
            // override to make sure we do not replace a symbol twice
            return inputColumn;
        }

        public void process(List<Symbol> symbols, Context context) {
            if (symbols != null) {
                for (int i = 0; i < symbols.size(); i++) {
                    symbols.set(i, process(symbols.get(i), context));
                }
            }
        }

        @Override
        public Symbol visitReference(Reference symbol, Context context) {
            return super.visitReference(symbol, context);
        }

        @Override
        public Symbol visitAggregation(Aggregation symbol, Context context) {
            return context.allocateAggregation(symbol);
        }

        @Override
        public Symbol visitFunction(Function function, Context context) {
            if (function.info().isAggregate()) {
                // split off below aggregates
                for (int i = 0; i < function.arguments().size(); i++) {
                    Symbol ic = process(function.arguments().get(i), context);
                    // note, that this modifies the tree
                    function.arguments().set(i, ic);
                }
                return context.allocateAggregation(function);
            } else {
                return context.allocateSymbol(function);
            }
        }
    }

    private static final SplittingPlanNodeVisitor nodeVisitor = new SplittingPlanNodeVisitor();

    public Plan plan(Analysis analysis) {
        analysis.query();

        Plan plan = new Plan();

        if (analysis.hasAggregates()) {
            if (analysis.hasGroupBy()) {
                throw new UnsupportedOperationException("groupd aggregates query plan not implemented");
            } else {
                // global aggregate: collect and partial aggregate on C and final agg on H
                Context context = new Context(Aggregation.Step.PARTIAL, analysis.hasGroupBy());
                nodeVisitor.process(analysis.outputSymbols(), context);
                nodeVisitor.process(analysis.groupBy(), context);
                nodeVisitor.process(analysis.sortSymbols(), context);

                CollectNode collectNode = new CollectNode();
                collectNode.routing(analysis.routing());
                collectNode.whereClause(analysis.whereClause());
                collectNode.toCollect(context.symbolList());

                AggregationProjection ap = new AggregationProjection();
                ap.aggregations(context.aggregationList());
                collectNode.projections(ImmutableList.<Projection>of(ap));

                plan.add(collectNode);

                // the hander stuff
                Context mergeContext = new Context(Aggregation.Step.FINAL, analysis.hasGroupBy());
                nodeVisitor.process(analysis.outputSymbols(), mergeContext);
                nodeVisitor.process(analysis.groupBy(), mergeContext);
                nodeVisitor.process(analysis.sortSymbols(), mergeContext);

                MergeNode mergeNode = new MergeNode();
                ap = new AggregationProjection();
                ap.aggregations(mergeContext.aggregationList());
                mergeNode.projections(ImmutableList.<Projection>of(ap));
                plan.add(mergeNode);
            }
        } else {
            if (analysis.hasGroupBy()) {
                throw new UnsupportedOperationException("groups without aggregates query plan not implemented");
            } else {
                if (analysis.rowGranularity().ordinal() >= RowGranularity.DOC.ordinal()) {
                    // this is an es query
                    // this only supports references as order by
                    List<Reference> orderBy;
                    if (analysis.isSorted()) {
                        orderBy = Lists.transform(analysis.sortSymbols(), new com.google.common.base.Function<Symbol, Reference>() {
                            @Override
                            public Reference apply(Symbol input) {
                                Preconditions.checkArgument(input.symbolType() == SymbolType.REFERENCE,
                                        "Unsupported order symbol for ESPlan", input);
                                return (Reference) input;
                            }
                        });

                    } else {
                        orderBy = null;
                    }
                    ESSearchNode node = new ESSearchNode(
                            analysis.outputSymbols(),
                            orderBy,
                            analysis.reverseFlags(),
                            analysis.limit(),
                            analysis.offset(),
                            analysis.whereClause()
                    );
                    plan.add(node);
                } else {
                    // node or shard level normal select
                    Context context = new Context(null, false);
                    nodeVisitor.process(analysis.outputSymbols(), context);
                    nodeVisitor.process(analysis.sortSymbols(), context);

                    CollectNode collectNode = new CollectNode();
                    collectNode.routing(analysis.routing());
                    collectNode.whereClause(analysis.whereClause());
                    collectNode.toCollect(context.symbolList());

                    plan.add(collectNode);

                    if (analysis.limit() != null) {
                        TopNProjection tnp = new TopNProjection(
                                analysis.limit(), analysis.offset(), analysis.sortSymbols(), analysis.reverseFlags());
                        tnp.outputs(analysis.outputSymbols());
                        collectNode.projections(ImmutableList.<Projection>of(tnp));
                    }

                    // TODO: nodes() for merge node to tell where to run
                    // TODO: num upstreams
                    nodeVisitor.process(analysis.outputSymbols(), context);
                    nodeVisitor.process(analysis.sortSymbols(), context);
                    MergeNode mergeNode = new MergeNode();
                    TopNProjection tnp = new TopNProjection(
                            analysis.limit(), analysis.offset(), analysis.sortSymbols(), analysis.reverseFlags());
                    tnp.outputs(analysis.outputSymbols());
                    mergeNode.projections(ImmutableList.<Projection>of(tnp));
                    plan.add(mergeNode);
                }
            }
        }
        return plan;
    }
}
