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

package io.crate.execution.engine.collect;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.IntegTestCase;
import org.junit.Test;

import io.crate.execution.engine.collect.sources.ShardCollectSource;

@IntegTestCase.ClusterScope(numClientNodes = 0, numDataNodes = 1, supportsDedicatedMasters = false)
public class ShardCollectorProviderTest extends IntegTestCase {

    public void assertNumberOfShardEntriesInShardCollectSource(int numberOfShards) throws Exception {
        final Field shards = ShardCollectSource.class.getDeclaredField("shards");
        shards.setAccessible(true);
        final List<ShardCollectSource> shardCollectSources = StreamSupport.stream(
            cluster().getInstances(ShardCollectSource.class).spliterator(), false)
            .collect(Collectors.toList());
        for (ShardCollectSource shardCollectSource : shardCollectSources) {
            try {
                //noinspection unchecked
                Map<ShardId, ShardCollectorProvider> shardMap = (Map<ShardId, ShardCollectorProvider>) shards.get(shardCollectSource);
                assertThat(shardMap).hasSize(numberOfShards);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Test
    public void testClosedIndicesHaveShardEntries() throws Exception {
        var numberOfShards = randomIntBetween(1, 4);
        execute("create table t(i int) clustered into ? shards with (number_of_replicas='0-all')", new Object[]{numberOfShards});
        ensureGreen();
        execute("alter table t close");
        waitUntilShardOperationsFinished();
        assertNumberOfShardEntriesInShardCollectSource(numberOfShards);
    }

    @Test
    public void testDeletedIndicesHaveNoShardEntries() throws Exception {
        execute("create table tt(i int) with (number_of_replicas='0-all')");
        ensureGreen();
        execute("drop table tt");
        waitUntilShardOperationsFinished();
        assertNumberOfShardEntriesInShardCollectSource(0);
    }

    @Test
    public void testQuerySysShardsWhileDropTable() throws Exception {
        execute("create table t1 (x int)");
        execute("create table t2 (x int)");
        execute("insert into t1 values (1), (2), (3)");
        execute("insert into t2 values (4), (5), (6)");
        ensureYellow();
        PlanForNode plan = plan("select * from sys.shards");
        execute("drop table t1");

        // shouldn't throw an exception:
        execute(plan).getResult();
    }
}
