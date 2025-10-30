/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package org.elasticsearch.action.support.single.shard;

import static io.crate.testing.Asserts.assertThat;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

public class SingleShardRequestTest extends ESTestCase {

    @Test
    public void test_streaming() throws Exception {
        Index index = new Index("index-name", "index-uuid");
        ShardId shardId = new ShardId(index, 0);

        SingleShardRequest shardRequest = new SingleShardRequest(index) {};
        shardRequest.internalShardId = shardId;

        try (var out = new BytesStreamOutput()) {
            shardRequest.writeTo(out);
            try (var in = out.bytes().streamInput()) {
                SingleShardRequest shardRequest1 = new SingleShardRequest(in) {};
                Index index1 = shardRequest1.index();
                assertThat(index1.getName().equals(IndexMetadata.INDEX_NAME_NA_VALUE));
                assertThat(index1.getUUID().equals(index.getUUID()));
                assertThat(shardRequest1.internalShardId).isEqualTo(shardId);
            }
        }
    }

    @Test
    public void test_streaming_bwc_before_6_1() throws Exception {
        Index index = new Index("index-name", "index-uuid");
        ShardId shardId = new ShardId(index, 0);

        try (var out = new BytesStreamOutput()) {
            TaskId.EMPTY_TASK_ID.writeTo(out);
            out.writeBoolean(true);
            shardId.writeTo(out);
            out.writeOptionalString(index.getName());

            try (var in = out.bytes().streamInput()) {
                in.setVersion(Version.V_6_0_0);
                SingleShardRequest shardRequest1 = new SingleShardRequest(in) {};
                Index index1 = shardRequest1.index();
                assertThat(index1.getName().equals(index.getName()));
                assertThat(index1.getUUID().equals(IndexMetadata.INDEX_UUID_NA_VALUE));
            }
        }

        SingleShardRequest shardRequest = new SingleShardRequest(index) {};
        shardRequest.internalShardId = shardId;
        try (var out = new BytesStreamOutput()) {
            out.setVersion(Version.V_6_0_0);
            shardRequest.writeTo(out);
            try (var in = out.bytes().streamInput()) {
                in.setVersion(Version.V_6_0_0);
                TaskId.readFromStream(in);
                in.readBoolean();
                ShardId shardId1 = new ShardId(in);
                String indexName = in.readOptionalString();

                assertThat(shardId1).isEqualTo(shardId);
                assertThat(indexName).isEqualTo(index.getName());
            }
        }
    }
}
