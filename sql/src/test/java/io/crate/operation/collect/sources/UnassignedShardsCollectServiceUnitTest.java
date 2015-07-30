/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.index.shard.ShardId;
import org.junit.Test;

import java.util.HashSet;

public class UnassignedShardsCollectServiceUnitTest extends CrateUnitTest {

    @Test
    public void testIsPrimaryAllShardsUnassinged() throws Exception {
        UnassignedShardsCollectSource.UnassignedShardIteratorContext context =
                new UnassignedShardsCollectSource(null, null, null).new UnassignedShardIteratorContext();
        // Set<ShardId> must be instantiated but empty,
        // which means: no primary shards available; all shards unassigned for a specific table name and shard id
        context.seenPrimaries = new HashSet<>();

        assertTrue(context.isPrimary(new ShardId("index", 1)));
        assertEquals(1, context.seenPrimaries.size());
        assertFalse(context.isPrimary(new ShardId("index", 1)));
    }

}
