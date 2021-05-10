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


import org.elasticsearch.index.shard.ShardId;

import java.io.Closeable;
import java.io.IOException;
import java.util.UUID;

public class BlobTransferStatus implements Closeable {

    private final String index;
    private final UUID transferId;
    private final DigestBlob digestBlob;
    private final ShardId shardId;

    public BlobTransferStatus(ShardId shardId, UUID transferId, DigestBlob digestBlob) {
        this.shardId = shardId;
        this.index = shardId.getIndexName();
        this.transferId = transferId;
        this.digestBlob = digestBlob;
    }

    public String index() {
        return index;
    }

    public DigestBlob digestBlob() {
        return digestBlob;
    }

    public UUID transferId() {
        return transferId;
    }

    public ShardId shardId() {
        return shardId;
    }

    @Override
    public void close() throws IOException {
        digestBlob.close();
    }
}
