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

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import io.crate.analyze.HavingClause;
import io.crate.analyze.QueriedTable;
import io.crate.analyze.QueriedTableRelation;
import io.crate.analyze.QuerySpec;
import io.crate.analyze.relations.AbstractTableRelation;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.QueriedDocTable;
import io.crate.analyze.symbol.*;
import io.crate.analyze.symbol.format.SymbolFormatter;
import io.crate.exceptions.VersionInvalidException;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Functions;
import io.crate.metadata.Reference;
import io.crate.metadata.RowGranularity;
import io.crate.operation.projectors.TopN;
import io.crate.planner.Limits;
import io.crate.planner.Merge;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.node.dql.Collect;
import io.crate.planner.node.dql.MergePhase;
import io.crate.planner.node.dql.RoutedCollectPhase;
import io.crate.planner.projection.AggregationProjection;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.TopNProjection;
import io.crate.planner.projection.builder.ProjectionBuilder;
import io.crate.planner.projection.builder.SplitPoints;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;


class GlobalAggregateConsumer implements Consumer {

    private static final AggregationOutputValidator AGGREGATION_OUTPUT_VALIDATOR = new AggregationOutputValidator();
    private final RelationPlanningVisitor visitor;

    GlobalAggregateConsumer(Functions functions) {
        visitor = new Visitor(functions);
    }

    @Override
    public Plan consume(AnalyzedRelation rootRelation, ConsumerContext context) {
        return visitor.process(rootRelation, context);
    }

    private static class Visitor extends RelationPlanningVisitor {

        private final Functions functions;

        public Visitor(Functions functions) {
            this.functions = functions;
        }

        @Override
        public Plan visitQueriedDocTable(QueriedDocTable table, ConsumerContext context) {
            if (table.querySpec().where().hasVersions()) {
                context.validationException(new VersionInvalidException());
                return null;
            }
            return globalAggregates(functions, table, context, RowGranularity.SHARD);

        }

        @Override
        public Plan visitQueriedTable(QueriedTable table, ConsumerContext context) {
            return globalAggregates(functions, table, context, RowGranularity.CLUSTER);
        }

    }

    private static Plan globalAggregates(Functions functions,
                                                            QueriedTableRelation table,
                                                            ConsumerContext context,
                                                            RowGranularity projectionGranularity) {
        QuerySpec querySpec = table.querySpec();
        if (querySpec.groupBy().isPresent() || !querySpec.hasAggregates()) {
            return null;
        }

        Planner.Context plannerContext = context.plannerContext();
        validateAggregationOutputs(table.tableRelation(), querySpec.outputs());
        // global aggregate: collect and partial aggregate on C and final agg on H

        ProjectionBuilder projectionBuilder = new ProjectionBuilder(functions, querySpec);
        SplitPoints splitPoints = projectionBuilder.getSplitPoints();

        AggregationProjection ap = projectionBuilder.aggregationProjection(
            splitPoints.leaves(),
            splitPoints.aggregates(),
            Aggregation.Step.ITER,
            Aggregation.Step.PARTIAL,
            projectionGranularity
        );
        RoutedCollectPhase collectPhase = RoutedCollectPhase.forQueriedTable(
            plannerContext,
            table,
            splitPoints.leaves(),
            ImmutableList.of(ap)
        );
        Collect collect = new Collect(collectPhase, TopN.NO_LIMIT, 0, ap.outputs().size(), 1, null);

        //// the handler stuff
        List<Projection> mergeProjections = new ArrayList<>();
        mergeProjections.add(projectionBuilder.aggregationProjection(
            splitPoints.aggregates(),
            splitPoints.aggregates(),
            Aggregation.Step.PARTIAL,
            Aggregation.Step.FINAL,
            RowGranularity.CLUSTER));

        Optional<HavingClause> havingClause = querySpec.having();
        if (havingClause.isPresent()) {
            HavingClause having = havingClause.get();
            mergeProjections.add(ProjectionBuilder.filterProjection(splitPoints.aggregates(), having));
        }
        Limits limits = plannerContext.getLimits(querySpec);
        TopNProjection topNProjection = ProjectionBuilder.topNProjection(
            splitPoints.aggregates(),
            null,
            limits.offset(),
            limits.finalLimit(),
            querySpec.outputs()
        );
        mergeProjections.add(topNProjection);
        MergePhase mergePhase = new MergePhase(
            plannerContext.jobId(),
            plannerContext.nextExecutionPhaseId(),
            "mergeOnHandler",
            collectPhase.nodeIds().size(),
            Collections.emptyList(),
            Symbols.extractTypes(ap.outputs()),
            mergeProjections,
            DistributionInfo.DEFAULT_SAME_NODE,
            null
        );
        return new Merge(collect, mergePhase, TopN.NO_LIMIT, 0, topNProjection.outputs().size(), 1, null);
    }

    private static void validateAggregationOutputs(AbstractTableRelation tableRelation, Collection<? extends Symbol> outputSymbols) {
        OutputValidatorContext context = new OutputValidatorContext(tableRelation);
        for (Symbol outputSymbol : outputSymbols) {
            context.insideAggregation = false;
            AGGREGATION_OUTPUT_VALIDATOR.process(outputSymbol, context);
        }
    }

    private static class OutputValidatorContext {
        private final AbstractTableRelation tableRelation;
        private boolean insideAggregation = false;

        public OutputValidatorContext(AbstractTableRelation tableRelation) {
            this.tableRelation = tableRelation;
        }
    }

    private static class AggregationOutputValidator extends SymbolVisitor<OutputValidatorContext, Void> {

        @Override
        public Void visitFunction(Function symbol, OutputValidatorContext context) {
            context.insideAggregation =
                context.insideAggregation || symbol.info().type().equals(FunctionInfo.Type.AGGREGATE);
            for (Symbol argument : symbol.arguments()) {
                process(argument, context);
            }
            context.insideAggregation = false;
            return null;
        }

        @Override
        public Void visitReference(Reference symbol, OutputValidatorContext context) {
            if (context.insideAggregation) {
                Reference.IndexType indexType = symbol.indexType();
                if (indexType == Reference.IndexType.ANALYZED) {
                    throw new IllegalArgumentException(SymbolFormatter.format(
                        "Cannot select analyzed column '%s' within grouping or aggregations", symbol));
                } else if (indexType == Reference.IndexType.NO) {
                    throw new IllegalArgumentException(SymbolFormatter.format(
                        "Cannot select non-indexed column '%s' within grouping or aggregations", symbol));
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
