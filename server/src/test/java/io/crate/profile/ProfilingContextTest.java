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

package io.crate.profile;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.junit.Test;

public class ProfilingContextTest {

    @Test
    public void testProfilingKeyIsIdHyphenName() {
        String key = ProfilingContext.generateProfilingKey(0, "test");
        assertThat(key).isEqualTo("0-test");
    }

    @Test
    public void testProfilingKeyForNullNameIsIdHyphenNull() {
        String key = ProfilingContext.generateProfilingKey(1, null);
        assertThat(key).isEqualTo("1-null");
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testRecordCountTimingAndGetDurationInMSByTimer() {
        ClusterState clusterState = ClusterState.builder(new ClusterName("test")).metadata(Metadata.EMPTY_METADATA).build();
        ProfilingContext ctx = new ProfilingContext(Map.of(), clusterState);

        ctx.recordCountTiming("doc", "t", null, 1, "CountQuery", "*:*", 2_000_000L);
        ctx.recordCountTiming("doc", "t", null, 0, "CountQuery", "*:*", 1_500_000L);
        ctx.recordCountTiming("my_schema", "parted", "04130", 0, "CountQuery", "x:1", 3_000_000L);

        Map<String, Object> durations = ctx.getDurationInMSByTimer();
        assertThat(durations).containsKey("QueryBreakdown");

        List<Map<String, Object>> breakdown = (List<Map<String, Object>>) durations.get("QueryBreakdown");
        assertThat(breakdown).hasSize(3);

        // doc.t shard 0
        assertThat(breakdown.getFirst()).containsEntry("SchemaName", "doc");
        assertThat(breakdown.getFirst()).containsEntry("TableName", "t");
        assertThat(breakdown.getFirst()).containsEntry("ShardId", 0);
        assertThat(breakdown.getFirst()).containsEntry("QueryName", "CountQuery");
        assertThat(breakdown.getFirst()).containsEntry("QueryDescription", "*:*");
        assertThat(breakdown.get(0)).containsEntry("Time", 1.5);
        assertThat(breakdown.get(0)).doesNotContainKey("PartitionIdent");

        // doc.t shard 1
        assertThat(breakdown.get(1)).containsEntry("SchemaName", "doc");
        assertThat(breakdown.get(1)).containsEntry("TableName", "t");
        assertThat(breakdown.get(1)).containsEntry("ShardId", 1);
        assertThat(breakdown.get(1)).containsEntry("Time", 2.0);

        // my_schema.parted shard 0
        assertThat(breakdown.get(2)).containsEntry("SchemaName", "my_schema");
        assertThat(breakdown.get(2)).containsEntry("TableName", "parted");
        assertThat(breakdown.get(2)).containsEntry("PartitionIdent", "04130");
        assertThat(breakdown.get(2)).containsEntry("ShardId", 0);
        assertThat(breakdown.get(2)).containsEntry("QueryName", "CountQuery");
        assertThat(breakdown.get(2)).containsEntry("QueryDescription", "x:1");
        assertThat(breakdown.get(2)).containsEntry("Time", 3.0);
    }

    @Test
    public void testConcurrentCountTimingRecordingIsThreadSafe() throws Exception {
        ClusterState clusterState = ClusterState.builder(new ClusterName("test")).metadata(Metadata.EMPTY_METADATA).build();
        ProfilingContext ctx = new ProfilingContext(Map.of(), clusterState);

        int numThreads = 8;
        int perThreadCount = 50;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(numThreads);

        for (int i = 0; i < numThreads; i++) {
            final int threadIdx = i;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int j = 0; j < perThreadCount; j++) {
                        ctx.recordCountTiming("doc", "t", null, threadIdx * perThreadCount + j, "CountQuery", "*:*", 1_000_000L);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertThat(doneLatch.await(5, TimeUnit.SECONDS)).isTrue();
        executor.shutdown();

        assertThat(ctx.getCountTimings()).hasSize(numThreads * perThreadCount);
        Map<String, Object> durations = ctx.getDurationInMSByTimer();
        List<?> breakdown = (List<?>) durations.get("QueryBreakdown");
        assertThat(breakdown).hasSize(numThreads * perThreadCount);
    }
}
