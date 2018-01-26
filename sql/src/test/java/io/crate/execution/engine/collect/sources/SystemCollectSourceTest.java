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

package io.crate.execution.engine.collect.sources;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import io.crate.analyze.OrderBy;
import io.crate.analyze.WhereClause;
import io.crate.data.Row;
import io.crate.integrationtests.SQLTransportIntegrationTest;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.Routing;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TableIdent;
import io.crate.metadata.shard.unassigned.UnassignedShard;
import io.crate.metadata.sys.SysShardsTableInfo;
import io.crate.metadata.sys.SysTableDefinitions;
import io.crate.expression.reference.StaticTableReferenceResolver;
import io.crate.expression.reference.sys.snapshot.SysSnapshot;
import io.crate.expression.reference.sys.snapshot.SysSnapshots;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.execution.dsl.phases.RoutedCollectPhase;
import io.crate.types.DataTypes;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.snapshots.SnapshotException;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

@ESIntegTestCase.ClusterScope(numDataNodes = 1, supportsDedicatedMasters = false)
public class SystemCollectSourceTest extends SQLTransportIntegrationTest {

    private static final StaticTableReferenceResolver<UnassignedShard> REFERENCE_RESOLVER =
        new StaticTableReferenceResolver<>(SysShardsTableInfo.unassignedShardsExpressions());

    @Test
    public void testOrderBySymbolsDoNotAppearTwiceInRows() throws Exception {
        SystemCollectSource systemCollectSource = internalCluster().getDataNodeInstance(SystemCollectSource.class);

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
            DistributionInfo.DEFAULT_BROADCAST,
            null
        );
        collectPhase.orderBy(new OrderBy(Collections.singletonList(shardId), new boolean[]{false}, new Boolean[]{null}));

        Iterable<? extends Row> rows = systemCollectSource.toRowsIterableTransformation(collectPhase, REFERENCE_RESOLVER, false)
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
        SystemCollectSource systemCollectSource = internalCluster().getDataNodeInstance(SystemCollectSource.class);
        RoutedCollectPhase collectPhase = new RoutedCollectPhase(
            UUID.randomUUID(),
            1,
            "collect",
            new Routing(ImmutableMap.of()),
            RowGranularity.SHARD,
            ImmutableList.of(),
            ImmutableList.of(),
            WhereClause.MATCH_ALL,
            DistributionInfo.DEFAULT_BROADCAST,
            null);

        // No read isolation
        List<String> noReadIsolationIterable = new ArrayList<>();
        noReadIsolationIterable.add("a");
        noReadIsolationIterable.add("b");

        Iterable<? extends Row> rows = systemCollectSource.toRowsIterableTransformation(collectPhase, REFERENCE_RESOLVER, false)
            .apply(noReadIsolationIterable);
        assertThat(Iterables.size(rows), is(2));

        noReadIsolationIterable.add("c");
        assertThat(Iterables.size(rows), is(3));

        // Read isolation
        List<String> readIsolationIterable = new ArrayList<>();
        readIsolationIterable.add("a");
        readIsolationIterable.add("b");

        rows = systemCollectSource.toRowsIterableTransformation(collectPhase, REFERENCE_RESOLVER, true)
            .apply(readIsolationIterable);
        assertThat(Iterables.size(rows), is(2));

        readIsolationIterable.add("c");
        assertThat(Iterables.size(rows), is(2));
    }

    @Test
    public void testSnapshotSupplierExposesError() throws Exception {
        // SysSnapshots issues queries, so errors may occur. This test ensures that this errors are exposed.
        expectedException.expect(ExecutionException.class);
        expectedException.expectMessage(containsString("[my_repository] failed to find repository"));
        Supplier<CompletableFuture<? extends Iterable<SysSnapshot>>> snapshotSupplier = SysTableDefinitions.snapshotSupplier(
            new SysSnapshots(null, null) {

                @Override
                public Iterable<SysSnapshot> snapshotsGetter() throws SnapshotException, RepositoryException {
                    throw new RepositoryException("my_repository", "failed to find repository");
                }
            }
        );
        CompletableFuture future = snapshotSupplier.get();
        future.get(1, TimeUnit.SECONDS);
    }
}
