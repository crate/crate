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

package io.crate.execution.engine.collect.sources;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.StreamSupport;

import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.test.IntegTestCase;
import org.junit.Test;

import io.crate.analyze.OrderBy;
import io.crate.analyze.WhereClause;
import io.crate.data.Row;
import io.crate.execution.dsl.phases.RoutedCollectPhase;
import io.crate.expression.reference.StaticTableReferenceResolver;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.Routing;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.SimpleReference;
import io.crate.metadata.shard.unassigned.UnassignedShard;
import io.crate.metadata.sys.SysShardsTableInfo;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.types.DataTypes;

@IntegTestCase.ClusterScope(numDataNodes = 1, supportsDedicatedMasters = false)
public class SystemCollectSourceTest extends IntegTestCase {

    @Test
    public void testOrderBySymbolsDoNotAppearTwiceInRows() throws Exception {
        SystemCollectSource systemCollectSource = cluster().getDataNodeInstance(SystemCollectSource.class);

        SimpleReference shardId = new SimpleReference(
            new ReferenceIdent(new RelationName("sys", "shards"), "id"),
            RowGranularity.SHARD,
            DataTypes.INTEGER,
            0,
            null
        );

        RoutedCollectPhase collectPhase = new RoutedCollectPhase(
            UUID.randomUUID(),
            1,
            "collect",
            new Routing(Map.of()),
            RowGranularity.SHARD,
            Collections.singletonList(shardId),
            List.of(),
            WhereClause.MATCH_ALL.queryOrFallback(),
            DistributionInfo.DEFAULT_BROADCAST
        );
        collectPhase.orderBy(new OrderBy(Collections.singletonList(shardId), new boolean[]{false}, new boolean[]{false}));

        Iterable<? extends Row> rows = systemCollectSource.toRowsIterableTransformation(
            collectPhase,
            CoordinatorTxnCtx.systemTransactionContext(),
            unassignedShardRefResolver(),
            false
        ).apply(Collections.singletonList(new UnassignedShard(
            1,
            "foo",
            mock(ClusterService.class),
            true,
            ShardRoutingState.UNASSIGNED)));
        Row next = rows.iterator().next();
        assertThat(next.numColumns()).isEqualTo(1);
    }

    private StaticTableReferenceResolver<UnassignedShard> unassignedShardRefResolver() {
        return new StaticTableReferenceResolver<>(SysShardsTableInfo.unassignedShardsExpressions());
    }

    @Test
    public void testReadIsolation() throws Exception {
        SystemCollectSource systemCollectSource = cluster().getDataNodeInstance(SystemCollectSource.class);
        RoutedCollectPhase collectPhase = new RoutedCollectPhase(
            UUID.randomUUID(),
            1,
            "collect",
            new Routing(Map.of()),
            RowGranularity.SHARD,
            List.of(),
            List.of(),
            WhereClause.MATCH_ALL.queryOrFallback(),
            DistributionInfo.DEFAULT_BROADCAST);

        // No read isolation
        List<String> noReadIsolationIterable = new ArrayList<>();
        noReadIsolationIterable.add("a");
        noReadIsolationIterable.add("b");

        CoordinatorTxnCtx txnCtx = CoordinatorTxnCtx.systemTransactionContext();
        Iterable<? extends Row> rows = systemCollectSource.toRowsIterableTransformation(
            collectPhase, txnCtx, unassignedShardRefResolver(), false)
            .apply(noReadIsolationIterable);
        assertThat(StreamSupport.stream(rows.spliterator(), false).count()).isEqualTo(2L);

        noReadIsolationIterable.add("c");
        assertThat(StreamSupport.stream(rows.spliterator(), false).count()).isEqualTo(3L);

        // Read isolation
        List<String> readIsolationIterable = new ArrayList<>();
        readIsolationIterable.add("a");
        readIsolationIterable.add("b");

        rows = systemCollectSource.toRowsIterableTransformation(collectPhase, txnCtx, unassignedShardRefResolver(), true)
            .apply(readIsolationIterable);
        assertThat(StreamSupport.stream(rows.spliterator(), false).count()).isEqualTo(2L);

        readIsolationIterable.add("c");
        assertThat(StreamSupport.stream(rows.spliterator(), false).count()).isEqualTo(2L);
    }
}
