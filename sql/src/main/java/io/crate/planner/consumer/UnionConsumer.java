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

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.crate.analyze.OrderBy;
import io.crate.analyze.QuerySpec;
import io.crate.analyze.UnionSelect;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.PlanOutputSymbolExtractor;
import io.crate.analyze.relations.QueriedRelation;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.Symbols;
import io.crate.planner.Limits;
import io.crate.planner.Plan;
import io.crate.planner.PositionalOrderBy;
import io.crate.planner.node.dql.MergePhase;
import io.crate.planner.node.dql.UnionPhase;
import io.crate.planner.node.dql.UnionPlan;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.TopNProjection;
import io.crate.planner.projection.builder.ProjectionBuilder;
import org.elasticsearch.cluster.ClusterService;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

class UnionConsumer implements Consumer {

    private final Visitor visitor;

    UnionConsumer(ClusterService clusterService) {
        visitor = new Visitor(clusterService);
    }

    @Override
    public Plan consume(AnalyzedRelation rootRelation, ConsumerContext context) {
        return visitor.process(rootRelation, context);
    }

    private static class Visitor extends RelationPlanningVisitor {

        private final ClusterService clusterService;

        public Visitor(ClusterService clusterService) {
            this.clusterService = clusterService;
        }

        @Override
        public Plan visitUnionSelect(UnionSelect unionSelect, ConsumerContext context) {
            Set<String> handlerNodes = ImmutableSet.of(clusterService.localNode().id());
            Limits limits = context.plannerContext().getLimits(unionSelect.querySpec());
            List<MergePhase> mergePhases = new ArrayList<>();
            List<Plan> subPlans = new ArrayList<>();

            List<? extends Symbol> outputs = unionSelect.querySpec().outputs();

            for (QueriedRelation queriedRelation : unionSelect.relations()) {
                Plan subPlan = context.plannerContext().planSubRelation(queriedRelation, context);
                subPlans.add(subPlan);

                outputs = PlanOutputSymbolExtractor.extract(subPlan);
                QuerySpec querySpec = queriedRelation.querySpec();

                if (handlerNodes.equals(subPlan.resultDescription().nodeIds())) {
                    mergePhases.add(null);
                } else {
                    mergePhases.add(MergePhase.mergePhase(
                        context.plannerContext(),
                        handlerNodes,
                        subPlan.resultDescription().nodeIds().size(),
                        PositionalOrderBy.of(querySpec.orderBy().orNull(), querySpec.outputs()),
                        ImmutableList.of(),
                        Symbols.extractTypes(querySpec.outputs())));
                }
            }

            // Add a new symbol as first which acts as a placeholder for the
            // upstream inputId that is prepended in the union phase
            List<Symbol> outputsWithPrependedInputId = new ArrayList<>(unionSelect.querySpec().outputs());
            outputsWithPrependedInputId.add(0, Literal.ZERO);

            Optional<OrderBy> rootOrderBy = unionSelect.querySpec().orderBy();
            List<Projection> projections = ImmutableList.of();
            if (limits.hasLimit() || rootOrderBy.isPresent()) {
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
                unionSelect.querySpec().outputs(),
                handlerNodes);
            return new UnionPlan(unionPhase, subPlans);
        }
    }
}
