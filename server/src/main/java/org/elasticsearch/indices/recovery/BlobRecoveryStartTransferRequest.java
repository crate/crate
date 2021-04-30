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

package org.elasticsearch.indices.recovery;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

public class BlobRecoveryStartTransferRequest extends BlobRecoveryRequest {

    private static final AtomicLong TRANSFER_ID_GENERATOR = new AtomicLong();
    private final String path;
    private final BytesReference content;
    private final long size;
    private final long transferId;

    public BlobRecoveryStartTransferRequest(long recoveryId, String path, BytesArray content, long size) {
        super(recoveryId);
        this.path = path;
        this.content = content;
        this.size = size;
        this.transferId = TRANSFER_ID_GENERATOR.incrementAndGet();
    }

    public String path() {
        return path;
    }

    public BytesReference content() {
        return content;
    }

    public long size() {
        return size;
    }

    public long transferId() {
        return transferId;
    }

    public BlobRecoveryStartTransferRequest(StreamInput in) throws IOException {
        super(in);
        path = in.readString();
        content = in.readBytesReference();
        size = in.readVLong();
        transferId = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(path);
        out.writeBytesReference(content);
        out.writeVLong(size);
        out.writeVLong(transferId);
    }
}
