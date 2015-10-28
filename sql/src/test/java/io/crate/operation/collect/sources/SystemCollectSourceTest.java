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
import io.crate.analyze.OrderBy;
import io.crate.analyze.WhereClause;
import io.crate.analyze.symbol.Reference;
import io.crate.analyze.symbol.Symbol;
import io.crate.core.collections.Row;
import io.crate.metadata.*;
import io.crate.metadata.shard.unassigned.UnassignedShard;
import io.crate.operation.collect.StatsTables;
import io.crate.operation.reference.sys.check.SysChecker;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.node.dql.CollectPhase;
import io.crate.planner.projection.Projection;
import io.crate.types.DataTypes;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.discovery.DiscoveryService;
import org.elasticsearch.index.shard.ShardId;
import org.junit.Test;
import org.mockito.Answers;

import java.util.Collections;
import java.util.UUID;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

public class SystemCollectSourceTest {

    @Test
    public void testOrderBySymbolsDoNotAppearTwiceInRows() throws Exception {
        SystemCollectSource collectSource = new SystemCollectSource(
                mock(DiscoveryService.class),
                mock(Functions.class),
                mock(StatsTables.class, Answers.RETURNS_MOCKS.get()),
                mock(InformationSchemaIterables.class, Answers.RETURNS_MOCKS.get()),
                mock(SysChecker.class));

        Reference shardId = new Reference(new ReferenceInfo(
                new ReferenceIdent(new TableIdent("sys", "shards"), "id"), RowGranularity.SHARD, DataTypes.INTEGER));

        CollectPhase collectPhase = new CollectPhase(
                UUID.randomUUID(),
                1,
                "collect",
                new Routing(null),
                RowGranularity.SHARD,
                Collections.<Symbol>singletonList(shardId),
                ImmutableList.<Projection>of(),
                WhereClause.MATCH_ALL,
                DistributionInfo.DEFAULT_BROADCAST
        );
        collectPhase.orderBy(new OrderBy(Collections.<Symbol>singletonList(shardId), new boolean[] { false }, new Boolean[] { null }));
        Iterable<Row> rows = collectSource.toRowsIterable(collectPhase, Collections.singletonList(
                new UnassignedShard(new ShardId("foo", 1), mock(ClusterService.class), true, ShardRoutingState.UNASSIGNED)));
        Row next = rows.iterator().next();
        assertThat(next.size(), is(1));
    }
}