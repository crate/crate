/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.block;

import static java.util.EnumSet.copyOf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.elasticsearch.test.VersionUtils.randomVersion;
import static org.hamcrest.CoreMatchers.endsWith;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.elasticsearch.Version;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

public class ClusterBlockTests extends ESTestCase {

    public void testSerialization() throws Exception {
        int iterations = randomIntBetween(5, 20);
        for (int i = 0; i < iterations; i++) {
            Version version = randomVersion(random());
            ClusterBlock clusterBlock = randomClusterBlock(version);

            BytesStreamOutput out = new BytesStreamOutput();
            out.setVersion(version);
            clusterBlock.writeTo(out);

            StreamInput in = out.bytes().streamInput();
            in.setVersion(version);
            ClusterBlock result = new ClusterBlock(in);

            assertClusterBlockEquals(clusterBlock, result);
        }
    }

    public void testToStringDanglingComma() {
        final ClusterBlock clusterBlock = randomClusterBlock();
        assertThat(clusterBlock.toString(), not(endsWith(",")));
    }

    public void testGlobalBlocksCheckedIfNoIndicesSpecified() {
        ClusterBlock globalBlock = randomClusterBlock();
        ClusterBlocks clusterBlocks = new ClusterBlocks(Collections.singleton(globalBlock), ImmutableOpenMap.of());
        ClusterBlockException exception = clusterBlocks.indicesBlockedException(randomFrom(globalBlock.levels()), new String[0]);
        assertNotNull(exception);
        assertThat(Collections.singleton(globalBlock)).isEqualTo(exception.blocks());
    }

    public void testRemoveIndexBlockWithId() {
        final ClusterBlocks.Builder builder = ClusterBlocks.builder();
        builder.addIndexBlock("index-1",
                              new ClusterBlock(1, "uuid", "", true, true, true, RestStatus.OK, copyOf(ClusterBlockLevel.ALL)));
        builder.addIndexBlock("index-1",
                              new ClusterBlock(2, "uuid", "", true, true, true, RestStatus.OK, copyOf(ClusterBlockLevel.ALL)));
        builder.addIndexBlock("index-1",
                              new ClusterBlock(3, "uuid", "", true, true, true, RestStatus.OK, copyOf(ClusterBlockLevel.ALL)));
        builder.addIndexBlock("index-1",
                              new ClusterBlock(3, "other uuid", "", true, true, true, RestStatus.OK, copyOf(ClusterBlockLevel.ALL)));

        builder.addIndexBlock("index-2",
                              new ClusterBlock(3, "uuid3", "", true, true, true, RestStatus.OK, copyOf(ClusterBlockLevel.ALL)));

        ClusterBlocks clusterBlocks = builder.build();
        assertThat(clusterBlocks.indices().get("index-1")).hasSize(4);
        assertThat(clusterBlocks.indices().get("index-2")).hasSize(1);

        builder.removeIndexBlockWithId("index-1", 3);
        clusterBlocks = builder.build();

        assertThat(clusterBlocks.indices().get("index-1")).hasSize(2);
        assertThat(clusterBlocks.hasIndexBlockWithId("index-1", 1)).isTrue();
        assertThat(clusterBlocks.hasIndexBlockWithId("index-1", 2)).isTrue();
        assertThat(clusterBlocks.indices().get("index-2")).hasSize(1);
        assertThat(clusterBlocks.hasIndexBlockWithId("index-2", 3)).isTrue();

        builder.removeIndexBlockWithId("index-2", 3);
        clusterBlocks = builder.build();

        assertThat(clusterBlocks.indices().get("index-1")).hasSize(2);
        assertThat(clusterBlocks.hasIndexBlockWithId("index-1", 1)).isTrue();
        assertThat(clusterBlocks.hasIndexBlockWithId("index-1", 2)).isTrue();
        assertThat(clusterBlocks.indices().get("index-2")).isNull();
        assertThat(clusterBlocks.hasIndexBlockWithId("index-2", 3)).isFalse();
    }

    @Test
    public void testGetIndexBlockWithId() {
        final int blockId = randomInt();
        final ClusterBlock[] clusterBlocks = new ClusterBlock[randomIntBetween(1, 5)];

        final ClusterBlocks.Builder builder = ClusterBlocks.builder();
        for (int i = 0; i < clusterBlocks.length; i++) {
            clusterBlocks[i] = new ClusterBlock(blockId, "uuid" + i, "", true, true, true, RestStatus.OK, copyOf(ClusterBlockLevel.ALL));
            builder.addIndexBlock("index", clusterBlocks[i]);
        }

        assertThat(builder.build().indices().get("index")).hasSize(clusterBlocks.length);
        assertThat(builder.build().getIndexBlockWithId("index", blockId)).isIn(clusterBlocks);
        assertThat(builder.build().getIndexBlockWithId("index", randomValueOtherThan(blockId, ESTestCase::randomInt))).isNull();
    }

    private ClusterBlock randomClusterBlock() {
        return randomClusterBlock(randomVersion(random()));
    }

    private ClusterBlock randomClusterBlock(final Version version) {
        final String uuid = randomBoolean() ? UUIDs.randomBase64UUID() : null;
        final List<ClusterBlockLevel> levels = Arrays.asList(ClusterBlockLevel.values());
        return new ClusterBlock(randomInt(), uuid, "cluster block #" + randomInt(), randomBoolean(), randomBoolean(), randomBoolean(),
                                randomFrom(RestStatus.values()), copyOf(randomSubsetOf(randomIntBetween(1, levels.size()), levels)));
    }

    private void assertClusterBlockEquals(final ClusterBlock expected, final ClusterBlock actual) {
        assertThat(actual).isEqualTo(expected);
        assertThat(actual.id()).isEqualTo(expected.id());
        assertThat(actual.uuid()).isEqualTo(expected.uuid());
        assertThat(actual.status()).isEqualTo(expected.status());
        assertThat(actual.description()).isEqualTo(expected.description());
        assertThat(actual.retryable()).isEqualTo(expected.retryable());
        assertThat(actual.disableStatePersistence()).isEqualTo(expected.disableStatePersistence());
        assertArrayEquals(actual.levels().toArray(), expected.levels().toArray());
    }
}
