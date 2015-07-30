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

package io.crate.operation.collect.sources;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Multimaps;
import io.crate.metadata.Functions;
import io.crate.metadata.shard.unassigned.UnassignedShard;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.Input;
import io.crate.operation.RowDownstream;
import io.crate.operation.collect.CollectInputSymbolVisitor;
import io.crate.operation.collect.CrateCollector;
import io.crate.operation.collect.JobCollectContext;
import io.crate.operation.collect.RowsCollector;
import io.crate.operation.reference.sys.shard.unassigned.UnassignedShardsReferenceResolver;
import io.crate.planner.node.dql.CollectPhase;
import io.crate.planner.symbol.Literal;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndexMissingException;

import javax.annotation.Nullable;
import java.util.*;

public class UnassignedShardsCollectSource implements CollectSource {

    public static final IndexShardsEntryToShardIds INDEX_SHARDS_ENTRY_TO_SHARD_IDS = new IndexShardsEntryToShardIds();
    private final CollectInputSymbolVisitor<Input<?>> inputSymbolVisitor;
    private final static Iterable<UnassignedShard> NO_SHARDS = FluentIterable.from(ImmutableList.<UnassignedShard>of());

    private final ClusterService clusterService;

    @Inject
    @SuppressWarnings("unchecked")
    public UnassignedShardsCollectSource(Functions functions,
                                         ClusterService clusterService,
                                         UnassignedShardsReferenceResolver unassignedShardsReferenceResolver) {
        this.inputSymbolVisitor = new CollectInputSymbolVisitor(
                functions,
                unassignedShardsReferenceResolver
        );
        this.clusterService = clusterService;
    }

    private static class IndexShardsEntryToShardIds implements Function<Map.Entry<String, List<Integer>>, Iterable<? extends ShardId>> {
        @Nullable
        @Override
        public Iterable<? extends ShardId> apply(final Map.Entry<String, List<Integer>> entry) {
            return FluentIterable.from(entry.getValue()).transform(new Function<Integer, ShardId>() {
                @Nullable
                @Override
                public ShardId apply(Integer input) {
                    return new ShardId(entry.getKey(), input);
                }
            });
        }
    }

    protected class UnassignedShardIteratorContext {

        Set<ShardId> seenPrimaries;

        /**
         * Determine if <code>shardId</code> is a primary or secondary shard.
         */
        public boolean isPrimary(ShardId shardId) {
            if (seenPrimaries == null) {
                // be lazy
                // inspect all shards. add any primary shard for a id to the 'seen' set.
                seenPrimaries = new HashSet<>();
                for (ShardRouting shardRouting : clusterService.state().routingTable().allShards()) {
                    if (shardRouting.primary()) {
                        seenPrimaries.add(shardRouting.shardId());
                    }
                }
            }

            // if shardId can be added, there was no primary shard (hence, all shards for an id are unassigned).
            // -> add, return true: elect as primary shard.
            return seenPrimaries.add(shardId);
        }

    }

    private Iterable<UnassignedShard> createIterator(final Map<String, List<Integer>> indexShardMap) {
        String[] indices = indexShardMap.keySet().toArray(new String[indexShardMap.size()]);
        List<ShardRouting> allShards;
        try {
            allShards = clusterService.state().routingTable().allShards(indices);
        } catch (IndexMissingException e) {
            // edge case: index was deleted while collecting, no more shards
            return NO_SHARDS;
        }
        if (allShards == null || allShards.size() == 0) {
            return NO_SHARDS;
        }

        FluentIterable<ShardId> shardIdsFromRouting = FluentIterable
                .from(indexShardMap.entrySet())
                .transformAndConcat(INDEX_SHARDS_ENTRY_TO_SHARD_IDS);

        final ImmutableListMultimap<String, ShardRouting> shardsByNode = Multimaps.index(allShards, new Function<ShardRouting, String>() {
            @Nullable
            @Override
            public String apply(ShardRouting input) {
                return input.currentNodeId() == null ? TableInfo.NULL_NODE_ID : input.currentNodeId();
            }
        });
        final UnassignedShardIteratorContext context = new UnassignedShardIteratorContext();
        return shardIdsFromRouting.transform(new Function<ShardId, UnassignedShard>() {
            @Nullable
            @Override
            public UnassignedShard apply(ShardId input) {
                ImmutableList<ShardRouting> shardRoutings = shardsByNode.get(TableInfo.NULL_NODE_ID);
                if (shardRoutings == null) {
                    // shard state has changed since the routing got calculated and is now probably already active.
                    // display it as initializing anyway.
                    return new UnassignedShard(
                            input, clusterService, context.isPrimary(input), ShardRoutingState.INITIALIZING);
                }

                ShardRoutingState state = ShardRoutingState.UNASSIGNED;
                for (ShardRouting shardRouting : shardRoutings) {
                    if (shardRouting.shardId().equals(input)) {
                        state = shardRouting.state();
                        break;
                    }
                }
                return new UnassignedShard(
                        input, clusterService, context.isPrimary(input), state);
            }
        });
    }

    @Override
    @SuppressWarnings("unchecked")
    public Collection<CrateCollector> getCollectors(CollectPhase collectPhase, RowDownstream downstream, JobCollectContext jobCollectContext) {
        CollectInputSymbolVisitor.Context context = inputSymbolVisitor.extractImplementations(collectPhase);

        Map<String, Map<String, List<Integer>>> locations = collectPhase.routing().locations();
        assert locations != null : "locations must be present";
        Map<String, List<Integer>> indexShardMap = locations.get(TableInfo.NULL_NODE_ID);
        Iterable<UnassignedShard> iterable = createIterator(indexShardMap);

        Input<Boolean> condition;
        if (collectPhase.whereClause().hasQuery()) {
            condition = (Input<Boolean>) inputSymbolVisitor.process(collectPhase.whereClause().query(), context);
        } else {
            condition = Literal.newLiteral(true);
        }

        return ImmutableList.<CrateCollector>of(new RowsCollector<>(
                context.topLevelInputs(), context.docLevelExpressions(), downstream, iterable, condition));
    }
}
