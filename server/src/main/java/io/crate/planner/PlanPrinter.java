/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.index.shard.ShardId;
import org.jetbrains.annotations.Nullable;

import com.carrotsearch.hppc.IntIndexedContainer;
import com.carrotsearch.hppc.cursors.IntCursor;

import io.crate.analyze.OrderBy;
import io.crate.common.collections.Lists;
import io.crate.common.collections.MapBuilder;
import io.crate.execution.dsl.phases.AbstractProjectionsPhase;
import io.crate.execution.dsl.phases.CollectPhase;
import io.crate.execution.dsl.phases.CountPhase;
import io.crate.execution.dsl.phases.ExecutionPhase;
import io.crate.execution.dsl.phases.ExecutionPhaseVisitor;
import io.crate.execution.dsl.phases.FetchPhase;
import io.crate.execution.dsl.phases.HashJoinPhase;
import io.crate.execution.dsl.phases.JoinPhase;
import io.crate.execution.dsl.phases.MergePhase;
import io.crate.execution.dsl.phases.NestedLoopPhase;
import io.crate.execution.dsl.phases.PKLookupPhase;
import io.crate.execution.dsl.phases.RoutedCollectPhase;
import io.crate.execution.dsl.phases.UpstreamPhase;
import io.crate.execution.dsl.projection.Projection;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.Reference;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.node.dql.Collect;
import io.crate.planner.node.dql.CountPlan;
import io.crate.planner.node.dql.QueryThenFetch;
import io.crate.planner.node.dql.join.Join;

public final class PlanPrinter {

    private PlanPrinter() {
    }

    public static Map<String, Object> objectMap(ExecutionPlan executionPlan) {
        return ExecutionPlan2MapVisitor.createMap(executionPlan);
    }

    private static class ExecutionPhase2MapVisitor extends ExecutionPhaseVisitor<Void, MapBuilder<String, Object>> {

        public static final ExecutionPhase2MapVisitor INSTANCE = new ExecutionPhase2MapVisitor();

        private static MapBuilder<String, Object> createMap(ExecutionPhase executionPhase,
                                                            MapBuilder<String, Object> subMap) {
            return MapBuilder.<String, Object>treeMapBuilder()
                .put(executionPhase.type().toString(), subMap.map());
        }

        private static MapBuilder<String, Object> createSubMap(ExecutionPhase phase) {
            return MapBuilder.<String, Object>treeMapBuilder()
                .put("type", "executionPhase")
                .put("id", phase.phaseId())
                // Converting TreeMap.keySet() to be able to stream
                .put("executionNodes", new ArrayList<>(phase.nodeIds()));
        }

        static MapBuilder<String, Object> toBuilder(ExecutionPhase executionPhase) {
            assert executionPhase != null : "executionPhase must not be null";
            return executionPhase.accept(INSTANCE, null);
        }

        private static List<Map<String, Object>> projections(Iterable<Projection> projections) {
            // need to return a List because this is part of a map which is streamed to the client.
            // Map streaming uses StreamOutput#writeGenericValue which can't handle Iterable
            ArrayList<Map<String, Object>> result = new ArrayList<>();
            for (Projection projection : projections) {
                result.add(projection.mapRepresentation());
            }
            return result;
        }


        private ExecutionPhase2MapVisitor() {
        }

        @Override
        protected MapBuilder<String, Object> visitExecutionPhase(ExecutionPhase phase, Void context) {
            return createMap(phase, createSubMap(phase));
        }

        private MapBuilder<String, Object> process(DistributionInfo info) {
            return MapBuilder.<String, Object>treeMapBuilder()
                .put("distributedByColumn", info.distributeByColumn())
                .put("type", info.distributionType().toString());
        }

        private MapBuilder<String, Object> upstreamPhase(UpstreamPhase phase, MapBuilder<String, Object> b) {
            return b.put("distribution", process(phase.distributionInfo()).map());
        }

        private MapBuilder<String, Object> dqlPlanNode(AbstractProjectionsPhase phase, MapBuilder<String, Object> b) {
            if (phase.hasProjections()) {
                b.put("projections", projections(phase.projections()));
            }
            return b;
        }

        @Override
        public MapBuilder<String, Object> visitRoutedCollectPhase(RoutedCollectPhase phase, Void context) {
            MapBuilder<String, Object> builder = upstreamPhase(phase, createSubMap(phase));
            builder.put("toCollect", "[" + Lists.joinOn(", ", phase.toCollect(), Symbol::toString) + "]");
            dqlPlanNode(phase, builder);
            builder.put("routing", xContentSafeRoutingLocations(phase.routing().locations()));
            builder.put("where", phase.where().toString());
            OrderBy orderBy = phase.orderBy();
            if (orderBy != null) {
                builder.put("orderBy", orderBy.explainRepresentation());
            }
            return createMap(phase, builder);
        }

        @Override
        public MapBuilder<String, Object> visitPKLookup(PKLookupPhase phase, Void context) {
            MapBuilder<String, Object> builder = upstreamPhase(phase, createSubMap(phase));
            builder.put("toCollect", Lists.joinOn(", ", phase.toCollect(), Symbol::toString));
            dqlPlanNode(phase, builder);
            Map<String, List<String>> shardsByNode = new HashMap<>();
            for (String nodeId : phase.nodeIds()) {
                for (ShardId shardId : phase.getIdsByShardId(nodeId).keySet()) {
                    List<String> shards = shardsByNode.computeIfAbsent(nodeId, k -> new ArrayList<>());
                    shards.add(shardId.toString());
                }
            }
            builder.put("shardsByNode", shardsByNode);
            return createMap(phase, builder);
        }

        @Override
        public MapBuilder<String, Object> visitCollectPhase(CollectPhase phase, Void context) {
            MapBuilder<String, Object> builder = upstreamPhase(phase, createSubMap(phase));
            builder.put("toCollect", Lists.joinOn(", ", phase.toCollect(), Symbol::toString));
            return createMap(phase, builder);
        }

        @Override
        public MapBuilder<String, Object> visitCountPhase(CountPhase phase, Void context) {
            MapBuilder<String, Object> builder = upstreamPhase(phase, visitExecutionPhase(phase, context));
            builder.put("routing", xContentSafeRoutingLocations(phase.routing().locations()));
            builder.put("where", phase.where().toString());
            return builder;
        }

        @Override
        public MapBuilder<String, Object> visitFetchPhase(FetchPhase phase, Void context) {
            return createMap(phase, createSubMap(phase)
                .put("fetchRefs", Lists.joinOn(", ", phase.fetchRefs(), Reference::toString)));
        }

        @Override
        public MapBuilder<String, Object> visitMergePhase(MergePhase phase, Void context) {
            MapBuilder<String, Object> b = upstreamPhase(phase, createSubMap(phase));
            return createMap(phase, dqlPlanNode(phase, b));
        }

        @Override
        public MapBuilder<String, Object> visitNestedLoopPhase(NestedLoopPhase phase, Void context) {
            return getBuilderForJoinPhase(phase);
        }

        @Override
        public MapBuilder<String, Object> visitHashJoinPhase(HashJoinPhase phase, Void context) {
            return getBuilderForJoinPhase(phase);
        }

        private MapBuilder<String, Object> getBuilderForJoinPhase(JoinPhase phase) {
            MapBuilder<String, Object> b = upstreamPhase(
                phase,
                createSubMap(phase).put("joinType", phase.joinType()));
            return createMap(phase, dqlPlanNode(phase, b));
        }
    }

    private static class ExecutionPlan2MapVisitor extends ExecutionPlanVisitor<Void, MapBuilder<String, Object>> {

        private static final ExecutionPlan2MapVisitor INSTANCE = new ExecutionPlan2MapVisitor();

        private static MapBuilder<String, Object> createMap(ExecutionPlan executionPlan,
                                                            MapBuilder<String, Object> subMap) {
            return MapBuilder.<String, Object>treeMapBuilder()
                .put(executionPlan.getClass().getSimpleName(), subMap.map());
        }

        private static MapBuilder<String, Object> createSubMap() {
            return MapBuilder.<String, Object>treeMapBuilder().put("type", "executionPlan");
        }

        @Override
        protected MapBuilder<String, Object> visitPlan(ExecutionPlan executionPlan, Void context) {
            return createMap(executionPlan, createSubMap());
        }

        private static Map<String, Object> phaseMap(@Nullable ExecutionPhase node) {
            if (node == null) {
                return null;
            } else {
                return ExecutionPhase2MapVisitor.toBuilder(node).map();
            }
        }

        static Map<String, Object> createMap(ExecutionPlan executionPlan) {
            assert executionPlan != null : "plan must not be null";
            return INSTANCE.process(executionPlan, null).map();
        }

        @Override
        public MapBuilder<String, Object> visitCollect(Collect plan, Void context) {
            return createMap(plan, createSubMap()
                .put("collectPhase", phaseMap(plan.collectPhase())));
        }

        @Override
        public MapBuilder<String, Object> visitJoin(Join plan, Void context) {
            return createMap(plan, createSubMap()
                .put("left", process(plan.left(), context).map())
                .put("right", process(plan.right(), context).map())
                .put("joinPhase", phaseMap(plan.joinPhase())));
        }

        @Override
        public MapBuilder<String, Object> visitQueryThenFetch(QueryThenFetch plan, Void context) {
            return createMap(plan, createSubMap()
                .put("subPlan", createMap(plan.subPlan()))
                .put("fetchPhase", phaseMap(plan.fetchPhase())));
        }

        @Override
        public MapBuilder<String, Object> visitMerge(Merge merge, Void context) {
            return createMap(merge, createSubMap()
                .put("subPlan", createMap(merge.subPlan()))
                .put("mergePhase", phaseMap(merge.mergePhase())));
        }

        @Override
        public MapBuilder<String, Object> visitUnionPlan(UnionExecutionPlan unionExecutionPlan, Void context) {
            return createMap(unionExecutionPlan, createSubMap()
                .put("left", createMap(unionExecutionPlan.left()))
                .put("right", createMap(unionExecutionPlan.right()))
                .put("mergePhase", phaseMap(unionExecutionPlan.mergePhase())));
        }

        @Override
        public MapBuilder<String, Object> visitCountPlan(CountPlan countPlan, Void context) {
            return visitPlan(countPlan, context)
                .put("countPhase", phaseMap(countPlan.countPhase()))
                .put("mergePhase", phaseMap(countPlan.mergePhase()));
        }
    }

    /**
     * Converts the shardId's of each node->table from a {@link IntIndexedContainer} to a list of Integers as custom
     * classes are not supported by the {@link org.elasticsearch.common.xcontent.XContentBuilder}.
     */
    private static Map<String, Map<String, List<Integer>>> xContentSafeRoutingLocations(
        Map<String, Map<String, IntIndexedContainer>> locations) {
        HashMap<String, Map<String, List<Integer>>> safeLocations = new HashMap<>(locations.size(), 1f);
        for (Map.Entry<String, Map<String, IntIndexedContainer>> nodeEntry : locations.entrySet()) {
            HashMap<String, List<Integer>> tableShards = new HashMap<>(nodeEntry.getValue().size(), 1f);
            for (Map.Entry<String, IntIndexedContainer> tableEntry : nodeEntry.getValue().entrySet()) {
                ArrayList<Integer> shardList = new ArrayList<>(tableEntry.getValue().size());
                for (IntCursor cursor : tableEntry.getValue()) {
                    shardList.add(cursor.value);
                }
                // ensure a deterministic shard list by sorting it (important for test assertions but maybe also for apps)
                shardList.sort(Integer::compareTo);
                tableShards.put(tableEntry.getKey(), shardList);
            }
            safeLocations.put(nodeEntry.getKey(), tableShards);
        }
        return safeLocations;
    }
}
