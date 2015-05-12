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

package io.crate.operation.reference.sys.shard.blob;

import io.crate.blob.stats.BlobStats;
import io.crate.blob.v2.BlobShard;
import io.crate.core.CachedRef;
import io.crate.metadata.shard.blob.BlobShardReferenceImplementation;
import io.crate.operation.reference.sys.shard.SysShardExpression;
import org.elasticsearch.common.inject.Inject;

import java.util.concurrent.TimeUnit;

public class BlobShardSizeExpression extends SysShardExpression<Long> implements BlobShardReferenceImplementation<Long> {

    public static final String NAME = "size";

    private final BlobShard blobShard;
    private final CachedRef<BlobStats> blobStatsCache = new CachedRef<BlobStats>(10, TimeUnit.SECONDS) {
        @Override
        protected BlobStats refresh() {
            return blobShard.blobStats();
        }
    };

    @Inject
    public BlobShardSizeExpression(BlobShard blobShard) {
        this.blobShard = blobShard;
    }

    @Override
    public Long value() {
        return blobStatsCache.get().totalUsage();
    }
}
