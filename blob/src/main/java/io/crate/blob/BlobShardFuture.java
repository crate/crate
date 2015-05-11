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

package io.crate.blob;

import io.crate.blob.v2.BlobIndices;
import io.crate.blob.v2.BlobShard;
import org.elasticsearch.common.util.concurrent.BaseFuture;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesLifecycle;

public class BlobShardFuture extends BaseFuture<BlobShard> {

    public BlobShardFuture(final BlobIndices blobIndices, final IndicesLifecycle indicesLifecycle,
                           final String index, final int shardId)
    {
        BlobShard blobShard = blobIndices.blobShard(index, shardId);
        if (blobShard != null) {
            set(blobShard);
            return;
        }

        IndicesLifecycle.Listener listener = new IndicesLifecycle.Listener() {

            @Override
            public void afterIndexShardCreated(IndexShard indexShard) {
                super.afterIndexShardCreated(indexShard);
                if (indexShard.shardId().index().getName().equals(index)) {
                    set(blobIndices.blobShardSafe(index, shardId));
                    indicesLifecycle.removeListener(this);
                }
            }
        };
        indicesLifecycle.addListener(listener);

        /**
         * prevent race condition:
         * if the index is created before the listener is added
         * but after the first blobShard() call
         */
        if (!isDone()) {
            blobShard = blobIndices.blobShard(index, shardId);
            if (blobShard != null) {
                indicesLifecycle.removeListener(listener);
                set(blobShard);
            }
        }
    }
}
