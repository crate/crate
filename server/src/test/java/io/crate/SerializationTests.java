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

package io.crate;

import static io.crate.testing.Asserts.assertThat;

import java.util.UUID;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.blob.PutChunkReplicaRequest;

public class SerializationTests extends ESTestCase {


    @Test
    public void testPutChunkReplicaRequestSerialization() throws Exception {
        UUID transferId = UUID.randomUUID();

        PutChunkReplicaRequest requestOut = new PutChunkReplicaRequest(
            new ShardId("foo", UUIDs.randomBase64UUID(), 1),
            "nodeId",
            transferId,
            0,
            new BytesArray(new byte[]{0x65, 0x66}),
            false
        );
        requestOut.index("foo");
        BytesStreamOutput outputStream = new BytesStreamOutput();
        requestOut.writeTo(outputStream);
        StreamInput inputStream = outputStream.bytes().streamInput();

        PutChunkReplicaRequest requestIn = new PutChunkReplicaRequest(inputStream);

        assertThat(requestOut.currentPos).isEqualTo(requestIn.currentPos);
        assertThat(requestOut.isLast).isEqualTo(requestIn.isLast);
        assertThat(requestOut.content).isEqualTo(requestIn.content);
        assertThat(requestOut.transferId).isEqualTo(requestIn.transferId);
        assertThat(requestOut.index()).isEqualTo(requestIn.index());
    }
}
