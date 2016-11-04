/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

import io.crate.blob.PutChunkReplicaRequest;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.junit.Test;

import java.util.UUID;

public class SerializationTests extends CrateUnitTest {


    @Test
    public void testPutChunkReplicaRequestSerialization() throws Exception {
        BytesStreamOutput outputStream = new BytesStreamOutput();

        UUID transferId = UUID.randomUUID();

        PutChunkReplicaRequest requestOut = new PutChunkReplicaRequest();
        requestOut.index("foo");
        requestOut.transferId = transferId;
        requestOut.currentPos = 10;
        requestOut.isLast = false;
        requestOut.content = new BytesArray(new byte[]{0x65, 0x66});
        requestOut.sourceNodeId = "nodeId";

        requestOut.writeTo(outputStream);

        StreamInput inputStream = outputStream.bytes().streamInput();

        PutChunkReplicaRequest requestIn = new PutChunkReplicaRequest();
        requestIn.readFrom(inputStream);

        assertEquals(requestOut.currentPos, requestIn.currentPos);
        assertEquals(requestOut.isLast, requestIn.isLast);
        assertEquals(requestOut.content, requestIn.content);
        assertEquals(requestOut.transferId, requestIn.transferId);
        assertEquals(requestOut.index(), requestIn.index());
    }
}
