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

package io.crate.operation.collect;

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import io.crate.metadata.Functions;
import io.crate.metadata.shard.unassigned.UnassignedShard;
import io.crate.metadata.shard.unassigned.UnassignedShardCollectorExpression;
import io.crate.operation.Input;
import io.crate.operation.projectors.Projector;
import io.crate.operation.reference.DocLevelReferenceResolver;
import io.crate.operation.reference.sys.shard.unassigned.UnassignedShardsReferenceResolver;
import io.crate.planner.node.dql.CollectNode;
import io.crate.planner.symbol.BooleanLiteral;
import org.apache.lucene.search.CollectionTerminatedException;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.shard.ShardId;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class UnassignedShardsCollectService implements CollectService {

    private final CollectInputSymbolVisitor<Input<?>> inputSymbolVisitor;
    private final static Iterable<UnassignedShard> NO_SHARDS = FluentIterable.from(ImmutableList.<UnassignedShard>of());

    private final ClusterService clusterService;

    @Inject
    @SuppressWarnings("unchecked")
    public UnassignedShardsCollectService(Functions functions,
                                          ClusterService clusterService,
                                          UnassignedShardsReferenceResolver unassignedShardsReferenceResolver) {
        this.inputSymbolVisitor = new CollectInputSymbolVisitor<Input<?>>(
            functions,
            (DocLevelReferenceResolver)unassignedShardsReferenceResolver
        );
        this.clusterService = clusterService;
    }

    protected class UnassignedShardIteratorContext {

        Set<ShardId> seenPrimaries;

        /**
         * Determine if <code>shardId</code> is a primary or secondary shard.
         * @param shardId
         * @return
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

    private Iterable<UnassignedShard> createIterator() {
        List<ShardRouting> allShards = clusterService.state().routingTable().allShards();
        if (allShards == null || allShards.size() == 0) {
            return NO_SHARDS;
        }

        final UnassignedShardIteratorContext context = new UnassignedShardIteratorContext();
        return FluentIterable.from(allShards).transform(new Function<ShardRouting, UnassignedShard>() {
            @Nullable
            @Override
            public UnassignedShard apply(@Nullable ShardRouting input) {
                assert input != null;
                if (input.unassigned()) {
                    return new UnassignedShard(input.shardId(), context.isPrimary(input.shardId()));
                }
                return null;
            }
        }).filter(Predicates.notNull());
    }

    @Override
    @SuppressWarnings("unchecked")
    public CrateCollector getCollector(CollectNode node, Projector projector) {
        if (node.whereClause().noMatch()) {
            return CrateCollector.NOOP;
        }
        CollectInputSymbolVisitor.Context context = inputSymbolVisitor.process(node);

        Iterable<UnassignedShard> iterable = createIterator();

        Input<Boolean> condition;
        if (node.whereClause().hasQuery()) {
            condition = (Input<Boolean>) inputSymbolVisitor.process(node.whereClause().query(), context);
        } else {
            condition = BooleanLiteral.TRUE;
        }

        return new UnassignedShardsCollector(
            context.topLevelInputs(), context.docLevelExpressions(), projector, iterable, condition);
    }

    private class UnassignedShardsCollector<R> implements CrateCollector {

        private final List<Input<?>> inputs;
        private final List<UnassignedShardCollectorExpression<?>> collectorExpressions;
        private Projector downstream;
        private final Iterable<UnassignedShard> rows;
        private final Input<Boolean> condition;

        public UnassignedShardsCollector(List<Input<?>> inputs,
                                         List<UnassignedShardCollectorExpression<?>> collectorExpressions,
                                         Projector downstream,
                                         Iterable<UnassignedShard> rows,
                                         Input<Boolean> condition) {
            this.inputs = inputs;
            this.collectorExpressions = collectorExpressions;
            this.rows = rows;
            this.condition = condition;
            downstream(downstream);
        }

        @Override
        public void doCollect() throws Exception {
            for (UnassignedShard row : rows) {
                for (UnassignedShardCollectorExpression<?> collectorExpression : collectorExpressions) {
                    collectorExpression.setNextRow(row);
                }
                Boolean match = condition.value();
                if (match == null || !match) {
                    // no match
                    continue;
                }

                Object[] newRow = new Object[inputs.size()];
                int i = 0;
                for (Input<?> input : inputs) {
                    newRow[i++] = input.value();
                }
                if (!downstream.setNextRow(newRow)) {
                    // no more rows required, we can stop here
                    throw new CollectionTerminatedException();
                }
            }

            downstream.upstreamFinished();
        }

        @Override
        public void downstream(Projector downstream) {
            this.downstream = downstream;
            downstream.registerUpstream(this);
        }

        @Override
        public Projector downstream() {
            return downstream;
        }
    }
}
