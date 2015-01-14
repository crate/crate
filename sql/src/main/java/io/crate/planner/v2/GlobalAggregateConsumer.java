/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.planner.v2;

import com.google.common.collect.ImmutableList;
import io.crate.analyze.SelectAnalyzedStatement;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.PlannedAnalyzedRelation;
import io.crate.analyze.relations.RelationVisitor;
import io.crate.analyze.relations.TableRelation;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.ReferenceInfo;
import io.crate.planner.PlanNodeBuilder;
import io.crate.planner.PlannerContextBuilder;
import io.crate.planner.node.dql.CollectNode;
import io.crate.planner.node.dql.GlobalAggregateNode;
import io.crate.planner.node.dql.MergeNode;
import io.crate.planner.projection.*;
import io.crate.planner.symbol.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class GlobalAggregateConsumer implements Consumer {

    private final Visitor visitor;
    private static final AggregationOutputValidator AGGREGATION_OUTPUT_VALIDATOR = new AggregationOutputValidator();

    public GlobalAggregateConsumer() {
        visitor = new Visitor();
    }

    @Override
    public boolean consume(AnalyzedRelation rootRelation, ConsumerContext context) {
        AnalyzedRelation analyzedRelation = visitor.process(rootRelation, null);
        if (analyzedRelation != null) {
            context.rootRelation(analyzedRelation);
            return true;
        }
        return false;
    }

    private static class Visitor extends RelationVisitor<Void, PlannedAnalyzedRelation> {

        @Override
        public PlannedAnalyzedRelation visitSelectAnalyzedStatement(SelectAnalyzedStatement selectAnalyzedStatement, Void context) {
            if (selectAnalyzedStatement.hasGroupBy() || !selectAnalyzedStatement.hasAggregates()) {
                return null;
            }
            return globalAggregates(selectAnalyzedStatement, null);
        }

        @Override
        protected PlannedAnalyzedRelation visitAnalyzedRelation(AnalyzedRelation relation, Void context) {
            return null;
        }
    }

    public static PlannedAnalyzedRelation globalAggregates(SelectAnalyzedStatement statement, ColumnIndexWriterProjection indexWriterProjection){
        TableRelation tableRelation = ConsumingPlanner.getSingleTableRelation(statement.sources());
        if (tableRelation == null) {
            return null;
        }
        validateAggregationOutputs(tableRelation, statement.outputSymbols());
        // global aggregate: collect and partial aggregate on C and final agg on H
        PlannerContextBuilder contextBuilder = new PlannerContextBuilder(2).output(tableRelation.resolve(statement.outputSymbols()));

        // havingClause could be a Literal or Function.
        // if its a Literal and value is false, we'll never reach this point (no match),
        // otherwise (true value) having can be ignored
        Symbol havingClause = null;
        Symbol having = statement.havingClause();
        if (having != null && having instanceof Function) {
            havingClause = contextBuilder.having(tableRelation.resolveHaving(having));
        }

        AggregationProjection ap = new AggregationProjection();
        ap.aggregations(contextBuilder.aggregations());
        CollectNode collectNode = PlanNodeBuilder.collect(
                tableRelation.tableInfo(),
                tableRelation.resolve(statement.whereClause()),
                contextBuilder.toCollect(),
                ImmutableList.<Projection>of(ap)
        );
        contextBuilder.nextStep();

        //// the handler stuff
        List<Projection> projections = new ArrayList<>();
        projections.add(new AggregationProjection(contextBuilder.aggregations()));

        if (havingClause != null) {
            FilterProjection fp = new FilterProjection((Function)havingClause);
            fp.outputs(contextBuilder.passThroughOutputs());
            projections.add(fp);
        }

        if (contextBuilder.aggregationsWrappedInScalar || havingClause != null) {
            // will filter out optional having symbols which are not selected
            TopNProjection topNProjection = new TopNProjection(1, 0);
            topNProjection.outputs(contextBuilder.outputs());
            projections.add(topNProjection);
        }
        if (indexWriterProjection != null) {
            projections.add(indexWriterProjection);
        }
        MergeNode localMergeNode = PlanNodeBuilder.localMerge(projections, collectNode);
        return new GlobalAggregateNode(collectNode, localMergeNode);
    }

    private static void validateAggregationOutputs(TableRelation tableRelation, Collection<? extends Symbol> outputSymbols) {
        OutputValidatorContext context = new OutputValidatorContext(tableRelation);
        for (Symbol outputSymbol : outputSymbols) {
            context.insideAggregation = false;
            AGGREGATION_OUTPUT_VALIDATOR.process(outputSymbol, context);
        }
    }

    private static class OutputValidatorContext {
        private final TableRelation tableRelation;
        private boolean insideAggregation = false;

        public OutputValidatorContext(TableRelation tableRelation) {
            this.tableRelation = tableRelation;
        }
    }

    private static class AggregationOutputValidator extends SymbolVisitor<OutputValidatorContext, Void> {

        @Override
        public Void visitFunction(Function symbol, OutputValidatorContext context) {
            context.insideAggregation = context.insideAggregation || symbol.info().type().equals(FunctionInfo.Type.AGGREGATE);
            for (Symbol argument : symbol.arguments()) {
                process(argument, context);
            }
            context.insideAggregation = false;
            return null;
        }

        @Override
        public Void visitReference(Reference symbol, OutputValidatorContext context) {
            if (context.insideAggregation) {
                ReferenceInfo.IndexType indexType = symbol.info().indexType();
                if (indexType == ReferenceInfo.IndexType.ANALYZED) {
                    throw new IllegalArgumentException(String.format(
                            "Cannot select analyzed column '%s' within grouping or aggregations", SymbolFormatter.format(symbol)));
                } else if (indexType == ReferenceInfo.IndexType.NO) {
                    throw new IllegalArgumentException(String.format(
                            "Cannot select non-indexed column '%s' within grouping or aggregations", SymbolFormatter.format(symbol)));
                }
            }
            return null;
        }

        @Override
        public Void visitField(Field field, OutputValidatorContext context) {
            return process(context.tableRelation.resolveField(field), context);
        }

        @Override
        protected Void visitSymbol(Symbol symbol, OutputValidatorContext context) {
            return null;
        }
    }
}
