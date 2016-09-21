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

package io.crate.planner.consumer;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.crate.analyze.OrderBy;
import io.crate.analyze.QuerySpec;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.PlannedAnalyzedRelation;
import io.crate.analyze.symbol.InputColumn;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.planner.Limits;
import io.crate.planner.Plan;
import io.crate.planner.RelationsUnion;
import io.crate.planner.node.dql.MergePhase;
import io.crate.planner.node.dql.UnionPhase;
import io.crate.planner.node.dql.UnionPlan;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.TopNProjection;
import io.crate.planner.projection.builder.ProjectionBuilder;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class UnionConsumer implements Consumer {

    private final Visitor visitor;

    @Inject
    public UnionConsumer(ClusterService clusterService) {
        visitor = new Visitor(clusterService);
    }

    @Override
    public PlannedAnalyzedRelation consume(AnalyzedRelation rootRelation, ConsumerContext context) {
        return visitor.process(rootRelation, context);
    }

    private static class Visitor extends RelationPlanningVisitor {

        private final ClusterService clusterService;

        public Visitor(ClusterService clusterService) {
            this.clusterService = clusterService;
        }

        @Override
        public PlannedAnalyzedRelation visitRelationsUnion(final RelationsUnion relationsUnion, ConsumerContext context) {
            Set<String> handlerNodes = ImmutableSet.of(clusterService.localNode().id());
            Limits limits = context.plannerContext().getLimits(true, relationsUnion.querySpec());
            List<MergePhase> mergePhases = new ArrayList<>();
            List<Plan> subPlans = new ArrayList<>();

            for (int i = 0; i < relationsUnion.relations().size(); i++) {
                PlannedAnalyzedRelation plannedAnalyzedRelation = relationsUnion.relations().get(i);
                QuerySpec querySpec = relationsUnion.querySpecs().get(i);
                subPlans.add(plannedAnalyzedRelation.plan());

                if (handlerNodes.equals(plannedAnalyzedRelation.resultPhase().executionNodes())) {
                    mergePhases.add(null);
                } else {
                    mergePhases.add(MergePhase.mergePhase(
                        context.plannerContext(),
                        handlerNodes,
                        plannedAnalyzedRelation.resultPhase().executionNodes().size(),
                        querySpec.orderBy().orNull(),
                        null,
                        ImmutableList.<Projection>of(),
                        querySpec.outputs(),
                        null));
                }
            }

            final List<Symbol> outputs = relationsUnion.querySpec().outputs();
            Optional<OrderBy> rootOrderBy = relationsUnion.querySpec().orderBy();
            List<Projection> projections = ImmutableList.of();
            if (limits.hasLimit() || rootOrderBy.isPresent()) {
                // Convert orderBy symbols referring to the 1st relation of the union to InputColumns
                // as rootOrderBy must apply to corresponding columns of all relations
                if (rootOrderBy.isPresent()) {
                    rootOrderBy.get().replace(new Function<Symbol, Symbol>() {
                        @Nullable
                        @Override
                        public Symbol apply(@Nullable Symbol symbol) {
                            return InputColumn.fromSymbol(symbol, outputs);
                        }
                    });
                }
                projections = new ArrayList<>(1);
                TopNProjection topN = ProjectionBuilder.topNProjection(
                    outputs,
                    rootOrderBy.orNull(),
                    limits.offset(),
                    limits.finalLimit(),
                    outputs
                );
                projections.add(topN);
            }

            UnionPhase unionPhase = new UnionPhase(context.plannerContext().jobId(),
                context.plannerContext().nextExecutionPhaseId(),
                "union",
                projections,
                mergePhases,
                outputs,
                handlerNodes);
            return new UnionPlan(unionPhase, subPlans);
        }
    }
}
