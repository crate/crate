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

package io.crate.planner.consumer;

import com.google.common.collect.ImmutableList;
import io.crate.analyze.*;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.relations.PlannedAnalyzedRelation;
import io.crate.analyze.relations.TableRelation;
import io.crate.exceptions.VersionInvalidException;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.ReferenceInfo;
import io.crate.planner.PlanNodeBuilder;
import io.crate.planner.node.NoopPlannedAnalyzedRelation;
import io.crate.planner.node.dql.CollectNode;
import io.crate.planner.node.dql.GlobalAggregate;
import io.crate.planner.node.dql.MergeNode;
import io.crate.planner.projection.AggregationProjection;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.TopNProjection;
import io.crate.planner.projection.builder.ProjectionBuilder;
import io.crate.planner.projection.builder.SplitPoints;
import io.crate.planner.symbol.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.google.common.base.MoreObjects.firstNonNull;


public class GlobalAggregateConsumer implements Consumer {

    private static final Visitor VISITOR = new Visitor();
    private static final AggregationOutputValidator AGGREGATION_OUTPUT_VALIDATOR = new AggregationOutputValidator();

    @Override
    public boolean consume(AnalyzedRelation rootRelation, ConsumerContext context) {
        AnalyzedRelation analyzedRelation = VISITOR.process(rootRelation, context);
        if (analyzedRelation != null) {
            context.rootRelation(analyzedRelation);
            return true;
        }
        return false;
    }

    private static class Visitor extends AnalyzedRelationVisitor<ConsumerContext, PlannedAnalyzedRelation> {

        @Override
        public PlannedAnalyzedRelation visitQueriedTable(QueriedTable table, ConsumerContext context) {
            if (table.querySpec().groupBy()!=null || !table.querySpec().hasAggregates()) {
                return null;
            }
            if (firstNonNull(table.querySpec().limit(), 1) < 1 || table.querySpec().offset() > 0){
                return new NoopPlannedAnalyzedRelation(table, context.plannerContext().jobId());
            }

            if (table.querySpec().where().hasVersions()){
                context.validationException(new VersionInvalidException());
                return null;
            }
            return globalAggregates(table, table.tableRelation(),  table.querySpec().where(), context);
        }

        @Override
        public PlannedAnalyzedRelation visitInsertFromQuery(InsertFromSubQueryAnalyzedStatement insertFromSubQueryAnalyzedStatement, ConsumerContext context) {
            InsertFromSubQueryConsumer.planInnerRelation(insertFromSubQueryAnalyzedStatement, context, this);
            return null;
        }

        @Override
        protected PlannedAnalyzedRelation visitAnalyzedRelation(AnalyzedRelation relation, ConsumerContext context) {
            return null;
        }
    }

    private static boolean noGroupBy(List<Symbol> groupBy) {
        return groupBy == null || groupBy.isEmpty();
    }

    public static PlannedAnalyzedRelation globalAggregates(QueriedTable table,
                                                           TableRelation tableRelation,
                                                           WhereClause whereClause,
                                                           ConsumerContext context) {
        assert noGroupBy(table.querySpec().groupBy()) : "must not have group by clause for global aggregate queries";
        validateAggregationOutputs(tableRelation, table.querySpec().outputs());
        // global aggregate: collect and partial aggregate on C and final agg on H

        ProjectionBuilder projectionBuilder = new ProjectionBuilder(table.querySpec());
        SplitPoints splitPoints = projectionBuilder.getSplitPoints();

        AggregationProjection ap = projectionBuilder.aggregationProjection(
                splitPoints.leaves(),
                splitPoints.aggregates(),
                Aggregation.Step.ITER,
                Aggregation.Step.PARTIAL);

        CollectNode collectNode = PlanNodeBuilder.collect(
                context.plannerContext().jobId(),
                tableRelation.tableInfo(),
                context.plannerContext(),
                whereClause,
                splitPoints.leaves(),
                ImmutableList.<Projection>of(ap)
        );

        //// the handler stuff
        List<Projection> projections = new ArrayList<>();
        projections.add(projectionBuilder.aggregationProjection(
                splitPoints.aggregates(),
                splitPoints.aggregates(),
                Aggregation.Step.PARTIAL,
                Aggregation.Step.FINAL));

        HavingClause havingClause = table.querySpec().having();
        if(havingClause != null){
            if (havingClause.noMatch()) {
                return new NoopPlannedAnalyzedRelation(table, context.plannerContext().jobId());
            } else if (havingClause.hasQuery()){
                projections.add(projectionBuilder.filterProjection(
                        splitPoints.aggregates(),
                        havingClause.query()
                ));
            }
        }

        TopNProjection topNProjection = projectionBuilder.topNProjection(
                splitPoints.aggregates(),
                null, 0, 1,
                table.querySpec().outputs()
                );
        projections.add(topNProjection);
        MergeNode localMergeNode = PlanNodeBuilder.localMerge(context.plannerContext().jobId(), projections, collectNode,
                context.plannerContext());
        return new GlobalAggregate(collectNode, localMergeNode, context.plannerContext().jobId());
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
