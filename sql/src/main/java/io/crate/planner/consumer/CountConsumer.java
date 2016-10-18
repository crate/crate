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
import io.crate.analyze.QuerySpec;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.QueriedDocTable;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.Symbols;
import io.crate.exceptions.VersionInvalidException;
import io.crate.metadata.Routing;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.aggregation.impl.CountAggregation;
import io.crate.planner.Limits;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.node.dql.CountPhase;
import io.crate.planner.node.dql.CountPlan;
import io.crate.planner.node.dql.MergePhase;
import io.crate.planner.projection.MergeCountProjection;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.TopNProjection;
import io.crate.types.DataTypes;

import java.util.Collections;
import java.util.List;

public class CountConsumer implements Consumer {

    private static final Visitor VISITOR = new Visitor();

    @Override
    public Plan consume(AnalyzedRelation rootRelation, ConsumerContext context) {
        return VISITOR.process(rootRelation, context);
    }

    private static class Visitor extends RelationPlanningVisitor {

        @Override
        public Plan visitQueriedDocTable(QueriedDocTable table, ConsumerContext context) {
            QuerySpec querySpec = table.querySpec();
            if (!querySpec.hasAggregates() || querySpec.groupBy().isPresent()) {
                return null;
            }
            if (!hasOnlyGlobalCount(querySpec.outputs())) {
                return null;
            }
            if (querySpec.where().hasVersions()) {
                context.validationException(new VersionInvalidException());
                return null;
            }

            TableInfo tableInfo = table.tableRelation().tableInfo();
            Planner.Context plannerContext = context.plannerContext();
            Routing routing = plannerContext.allocateRouting(tableInfo, querySpec.where(), null);

            CountPhase countPhase = new CountPhase(
                plannerContext.nextExecutionPhaseId(),
                routing,
                querySpec.where(),
                DistributionInfo.DEFAULT_BROADCAST);

            List<Projection> projections;
            Limits limits = context.plannerContext().getLimits(querySpec);
            TopNProjection topN = TopNProjection.createIfNeeded(
                limits.finalLimit(), limits.offset(), 1, Symbols.extractTypes(MergeCountProjection.INSTANCE.outputs()));
            if (topN == null) {
                projections = Collections.singletonList(MergeCountProjection.INSTANCE);
            } else {
                projections = ImmutableList.of(MergeCountProjection.INSTANCE, topN);
            }

            MergePhase mergePhase = new MergePhase(
                plannerContext.jobId(),
                plannerContext.nextExecutionPhaseId(),
                "count-merge",
                countPhase.nodeIds().size(),
                // if not root relation then no direct result can be used
                table == context.rootRelation() ?
                    Collections.emptyList() :
                    Collections.singletonList(context.plannerContext().handlerNode()),
                Collections.singletonList(DataTypes.LONG),
                projections,
                DistributionInfo.DEFAULT_SAME_NODE,
                null
            );
            return new CountPlan(countPhase, mergePhase);
        }

        private boolean hasOnlyGlobalCount(List<Symbol> symbols) {
            if (symbols.size() != 1) {
                return false;
            }
            Symbol symbol = symbols.get(0);
            if (!(symbol instanceof Function)) {
                return false;
            }
            Function function = (Function) symbol;
            return function.info().equals(CountAggregation.COUNT_STAR_FUNCTION);
        }
    }
}
