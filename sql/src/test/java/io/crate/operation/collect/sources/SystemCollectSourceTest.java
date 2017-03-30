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

package io.crate.operation.collect.sources;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import io.crate.analyze.OrderBy;
import io.crate.analyze.WhereClause;
import io.crate.data.Row;
import io.crate.integrationtests.SQLTransportIntegrationTest;
import io.crate.metadata.*;
import io.crate.metadata.shard.unassigned.UnassignedShard;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.node.dql.RoutedCollectPhase;
import io.crate.types.DataTypes;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

@ESIntegTestCase.ClusterScope(numDataNodes = 1)
public class SystemCollectSourceTest extends SQLTransportIntegrationTest {

    @Test
    public void testOrderBySymbolsDoNotAppearTwiceInRows() throws Exception {
        SystemCollectSource systemCollectSource = internalCluster().getInstance(SystemCollectSource.class);

        Reference shardId = new Reference(
            new ReferenceIdent(new TableIdent("sys", "shards"), "id"), RowGranularity.SHARD, DataTypes.INTEGER);

        RoutedCollectPhase collectPhase = new RoutedCollectPhase(
            UUID.randomUUID(),
            1,
            "collect",
            new Routing(ImmutableMap.of()),
            RowGranularity.SHARD,
            Collections.singletonList(shardId),
            ImmutableList.of(),
            WhereClause.MATCH_ALL,
            DistributionInfo.DEFAULT_BROADCAST
        );
        collectPhase.orderBy(new OrderBy(Collections.singletonList(shardId), new boolean[]{false}, new Boolean[]{null}));
        Iterable<? extends Row> rows = systemCollectSource.toRowsIterableTransformation(collectPhase, false)
            .apply(Collections.singletonList(new UnassignedShard(
                new ShardId("foo", UUIDs.randomBase64UUID(),1),
                mock(ClusterService.class),
                true,
                ShardRoutingState.UNASSIGNED)));
        Row next = rows.iterator().next();
        assertThat(next.numColumns(), is(1));
    }

    @Test
    public void testReadIsolation() throws Exception {
        SystemCollectSource systemCollectSource = internalCluster().getInstance(SystemCollectSource.class);
        RoutedCollectPhase collectPhase = new RoutedCollectPhase(
            UUID.randomUUID(),
            1,
            "collect",
            new Routing(ImmutableMap.of()),
            RowGranularity.SHARD,
            ImmutableList.of(),
            ImmutableList.of(),
            WhereClause.MATCH_ALL,
            DistributionInfo.DEFAULT_BROADCAST);

        // No read isolation
        List<String> noReadIsolationIterable = new ArrayList<>();
        noReadIsolationIterable.add("a");
        noReadIsolationIterable.add("b");

        Iterable<? extends Row> rows = systemCollectSource.toRowsIterableTransformation(collectPhase, false)
            .apply(noReadIsolationIterable);
        assertThat(Iterables.size(rows), is(2));

        noReadIsolationIterable.add("c");
        assertThat(Iterables.size(rows), is(3));

        // Read isolation
        List<String> readIsolationIterable = new ArrayList<>();
        readIsolationIterable.add("a");
        readIsolationIterable.add("b");

        rows = systemCollectSource.toRowsIterableTransformation(collectPhase, true)
            .apply(readIsolationIterable);
        assertThat(Iterables.size(rows), is(2));

        readIsolationIterable.add("c");
        assertThat(Iterables.size(rows), is(2));
    }
}
