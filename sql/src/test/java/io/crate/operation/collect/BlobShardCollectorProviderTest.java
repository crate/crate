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

package io.crate.operation.collect;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import io.crate.analyze.WhereClause;
import io.crate.blob.v2.BlobIndicesService;
import io.crate.blob.v2.BlobShard;
import io.crate.data.Row;
import io.crate.integrationtests.SQLHttpIntegrationTest;
import io.crate.metadata.Routing;
import io.crate.metadata.RowGranularity;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.node.dql.RoutedCollectPhase;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.CheckedRunnable;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.UUID;

import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class BlobShardCollectorProviderTest extends SQLHttpIntegrationTest {

    private BlobShardCollectorProvider collectorProvider;

    private Iterable<Row> getBlobRows(RoutedCollectPhase phase, boolean repeat) throws Exception {
        Method m = BlobShardCollectorProvider.class.getDeclaredMethod("getBlobRows", RoutedCollectPhase.class, boolean.class);
        m.setAccessible(true);
        //noinspection unchecked
        return (Iterable<Row>) m.invoke(collectorProvider, phase, repeat);
    }

    @Test
    public void testReadIsolation() throws Exception {
        execute("create blob table b1 clustered into 1 shards with (number_of_replicas = 0)");
        upload("b1", "foo");
        upload("b1", "bar");
        ensureGreen();
        assertBusy(new Initializer());

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
            null
        );

        // No read Isolation
        Iterable<Row> iterable = getBlobRows(collectPhase, false);
        assertThat(Iterables.size(iterable), is(2));
        upload("b1", "newEntry1");
        assertThat(Iterables.size(iterable), is(3));

        // Read isolation
        iterable = getBlobRows(collectPhase, true);
        assertThat(Iterables.size(iterable), is(3));
        upload("b1", "newEntry2");
        assertThat(Iterables.size(iterable), is(3));
    }

    private final class Initializer implements CheckedRunnable<Exception> {
        @Override
        public void run() {
            try {
                ClusterService clusterService = internalCluster().getDataNodeInstance(ClusterService.class);
                MetaData metaData = clusterService.state().getMetaData();
                String indexUUID = metaData.index(".blob_b1").getIndexUUID();
                BlobIndicesService blobIndicesService = internalCluster().getDataNodeInstance(BlobIndicesService.class);
                BlobShard blobShard = blobIndicesService.blobShard(new ShardId(".blob_b1", indexUUID, 0));
                assertNotNull(blobShard);
                collectorProvider = new BlobShardCollectorProvider(blobShard, null, null, null, null, null, null,
                    BigArrays.NON_RECYCLING_INSTANCE);
                assertNotNull(collectorProvider);
            } catch (Exception e) {
                fail("Exception shouldn't be thrown: " + e.getMessage());
            }
        }
    }
}
