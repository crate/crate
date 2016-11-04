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

package io.crate.blob.v2;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;

import java.nio.file.Path;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class BlobIndex {

    private static final String INDEX_PREFIX = ".blob_";

    /**
     * check if this index is a blob table
     * <p>
     * This only works for indices that were created via SQL.
     */
    public static boolean isBlobIndex(String indexName) {
        return indexName.startsWith(INDEX_PREFIX);
    }

    /**
     * Returns the full index name, adds blob index prefix.
     */
    public static String fullIndexName(String indexName) {
        if (isBlobIndex(indexName)) {
            return indexName;
        }
        return INDEX_PREFIX + indexName;
    }

    /**
     * Strips the blob index prefix from a full index name
     */
    public static String stripPrefix(String indexName) {
        if (!isBlobIndex(indexName)) {
            return indexName;
        }
        return indexName.substring(INDEX_PREFIX.length());
    }


    private final Map<Integer, BlobShard> shards = new ConcurrentHashMap<>();
    private final Path globalBlobPath;


    BlobIndex(@Nullable Path globalBlobPath) {
        this.globalBlobPath = globalBlobPath;
    }

    void createShard(IndexShard indexShard) {
        shards.put(indexShard.shardId().id(), new BlobShard(indexShard, globalBlobPath));
    }

    BlobShard removeShard(ShardId shardId) {
        BlobShard shard = shards.remove(shardId.id());
        if (shard != null) {
            shard.deleteShard();
        }
        return shard;
    }

    BlobShard getShard(int shardId) {
        return shards.get(shardId);
    }

    void delete() {
        Iterator<Map.Entry<Integer, BlobShard>> it = shards.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Integer, BlobShard> e = it.next();
            it.remove();
            e.getValue().deleteShard();
        }
    }
}
