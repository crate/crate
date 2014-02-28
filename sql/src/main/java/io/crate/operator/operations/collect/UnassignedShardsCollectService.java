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

package io.crate.operator.operations.collect;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import io.crate.metadata.Functions;
import io.crate.metadata.shard.unassigned.UnassignedShard;
import io.crate.metadata.shard.unassigned.UnassignedShardCollectorExpression;
import io.crate.operator.Input;
import io.crate.operator.collector.CrateCollector;
import io.crate.operator.operations.CollectInputSymbolVisitor;
import io.crate.operator.projectors.Projector;
import io.crate.operator.reference.DocLevelReferenceResolver;
import io.crate.operator.reference.sys.shard.unassigned.UnassignedShardsReferenceResolver;
import io.crate.planner.node.dql.CollectNode;
import io.crate.planner.symbol.BooleanLiteral;
import org.apache.lucene.search.CollectionTerminatedException;
import org.elasticsearch.common.inject.Inject;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class UnassignedShardsCollectService implements CollectService {

    private final CollectInputSymbolVisitor<Input<?>> inputSymbolVisitor;
    private final static Iterable<UnassignedShard> NO_SHARDS = FluentIterable.from(ImmutableList.<UnassignedShard>of());

    @Inject
    @SuppressWarnings("unchecked")
    public UnassignedShardsCollectService(Functions functions) {
        this.inputSymbolVisitor = new CollectInputSymbolVisitor<Input<?>>(
            functions,
            (DocLevelReferenceResolver)new UnassignedShardsReferenceResolver()
        );
    }

    private Iterable<UnassignedShard> createIterator(Map<String, Set<Integer>> tablesAndShards) {
        if (tablesAndShards == null) {
            return NO_SHARDS;
        }
        return FluentIterable.from(tablesAndShards.entrySet())
            .transformAndConcat(new Function<Map.Entry<String, Set<Integer>>, Iterable<UnassignedShard>>() {
                @Nullable
                @Override
                public Iterable<UnassignedShard> apply(@Nullable Map.Entry<String, Set<Integer>> input) {
                    assert input != null;
                    final String tableName = input.getKey();
                    return FluentIterable.from(input.getValue())
                        .transform(new Function<Integer, UnassignedShard>() {
                            @Nullable
                            @Override
                            public UnassignedShard apply(@Nullable Integer input) {
                                assert input != null;
                                return new UnassignedShard(tableName, input);
                            }
                        });
                }
            });
    }

    @Override
    @SuppressWarnings("unchecked")
    public CrateCollector getCollector(CollectNode node, Projector projector) {
        if (node.whereClause().noMatch()) {
            return CrateCollector.NOOP;
        }
        CollectInputSymbolVisitor.Context context = inputSymbolVisitor.process(node);

        Map<String,Set<Integer>> tablesAndShards = node.routing().locations().get(null);
        Iterable<UnassignedShard> iterable = createIterator(tablesAndShards);

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
        private final Projector downStream;
        private final Iterable<UnassignedShard> rows;
        private final Input<Boolean> condition;

        public UnassignedShardsCollector(List<Input<?>> inputs,
                                         List<UnassignedShardCollectorExpression<?>> collectorExpressions,
                                         Projector downStream,
                                         Iterable<UnassignedShard> rows,
                                         Input<Boolean> condition) {
            this.inputs = inputs;
            this.collectorExpressions = collectorExpressions;
            this.downStream = downStream;
            this.rows = rows;
            this.condition = condition;
        }

        @Override
        public void doCollect() throws Exception {
            for (UnassignedShard row : rows) {
                for (UnassignedShardCollectorExpression<?> collectorExpression : collectorExpressions) {
                    collectorExpression.setNextRow(row);
                }
                if (!condition.value()) {
                    // no match
                    continue;
                }

                Object[] newRow = new Object[inputs.size()];
                int i = 0;
                for (Input<?> input : inputs) {
                    newRow[i++] = input.value();
                }
                if (!downStream.setNextRow(newRow)) {
                    // no more rows required, we can stop here
                    throw new CollectionTerminatedException();
                }
            }
        }
    }
}
