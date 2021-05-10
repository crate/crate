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

import io.crate.common.Hex;
import org.elasticsearch.action.support.replication.ReplicationRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

public class DeleteBlobRequest extends ReplicationRequest<DeleteBlobRequest> {

    private byte[] digest;

    public DeleteBlobRequest(ShardId shardId, byte[] digest) {
        super(shardId);
        this.digest = digest;
    }

    public DeleteBlobRequest() {
    }

    public DeleteBlobRequest(StreamInput in) throws IOException {
        super(in);
        digest = new byte[20];
        in.read(digest);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.write(digest);
    }

    public String id() {
        return Hex.encodeHexString(digest);
    }

    @Override
    public String toString() {
        return "DeleteBlobRequest{" +
               "digest=" + id() +
               '}';
    }
}
