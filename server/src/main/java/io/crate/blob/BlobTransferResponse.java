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

package io.crate.blob;

import java.io.IOException;

import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

public class BlobTransferResponse extends ReplicationResponse {

    // current size of the file on the target
    private long size;
    private RemoteDigestBlob.Status status;

    public RemoteDigestBlob.Status status() {
        return status;
    }

    public BlobTransferResponse status(RemoteDigestBlob.Status status) {
        this.status = status;
        return this;
    }

    public long size() {
        return size;
    }

    public BlobTransferResponse size(long size) {
        this.size = size;
        return this;
    }

    protected BlobTransferResponse() {}

    public BlobTransferResponse(StreamInput in) throws IOException {
        super(in);
        status = RemoteDigestBlob.Status.fromId(in.readByte());
        size = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeByte(status.id());
        out.writeVLong(size);
    }
}
