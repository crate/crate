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

package io.crate.executor.transport;

import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.shard.ShardId;
import org.junit.Test;

import java.util.UUID;

import static org.hamcrest.Matchers.equalTo;

public class ShardDeleteRequestTest extends CrateUnitTest {

    @Test
    public void testStreaming() throws Exception {
        ShardId shardId = new ShardId("test", UUIDs.randomBase64UUID(), 1);
        UUID jobId = UUID.randomUUID();
        ShardDeleteRequest request = new ShardDeleteRequest(shardId, "42", jobId);

        request.add(123, new ShardDeleteRequest.Item("99"));
        request.add(5, new ShardDeleteRequest.Item("42"));

        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        ShardDeleteRequest request2 = new ShardDeleteRequest();
        request2.readFrom(in);

        assertThat(request, equalTo(request2));
    }
}
