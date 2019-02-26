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

package io.crate.es.indices.recovery;

import io.crate.es.common.bytes.BytesArray;
import io.crate.es.common.bytes.BytesReference;
import io.crate.es.common.io.stream.StreamInput;
import io.crate.es.common.io.stream.StreamOutput;

import java.io.IOException;

public class BlobRecoveryChunkRequest extends BlobRecoveryRequest {

    private long transferId;
    private BytesReference content;
    private boolean isLast;

    public BlobRecoveryChunkRequest() {

    }

    public BlobRecoveryChunkRequest(long requestId, long transferId, BytesArray content, boolean isLast) {
        super(requestId);
        this.transferId = transferId;
        this.content = content;
        this.isLast = isLast;
    }

    public BytesReference content() {
        return content;
    }

    public long transferId() {
        return transferId;
    }

    public boolean isLast() {
        return isLast;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        transferId = in.readVLong();
        content = in.readBytesReference();
        isLast = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVLong(transferId);
        out.writeBytesReference(content);
        out.writeBoolean(isLast);
    }
}
