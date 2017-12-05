/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.operation;

import io.crate.Constants;
import io.crate.data.BatchIterator;
import io.crate.data.InMemoryBatchIterator;
import io.crate.planner.operators.PKAndVersion;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public final class PKLookupOperation {

    private final IndicesService indicesService;

    public PKLookupOperation(IndicesService indicesService) {
        this.indicesService = indicesService;
    }

    public BatchIterator<GetResult> lookup(boolean ignoreMissing, Map<ShardId, List<PKAndVersion>> idsByShard) {
        Stream<GetResult> getResultStream = idsByShard.entrySet().stream()
            .flatMap(entry -> {
                ShardId shardId = entry.getKey();
                IndexService indexService = indicesService.indexService(shardId.getIndex());
                if (indexService == null) {
                    if (ignoreMissing) {
                        return Stream.empty();
                    }
                    throw new IndexNotFoundException(shardId.getIndex());
                }
                IndexShard shard = indexService.getShardOrNull(shardId.id());
                if (shard == null) {
                    if (ignoreMissing) {
                        return Stream.empty();
                    }
                    throw new ShardNotFoundException(shardId);
                }
                return entry.getValue().stream()
                    .map(pkAndVersion -> shard.getService().get(
                        Constants.DEFAULT_MAPPING_TYPE,
                        pkAndVersion.id(),
                        new String[0],
                        true,
                        pkAndVersion.version(),
                        VersionType.EXTERNAL,
                        FetchSourceContext.FETCH_SOURCE
                    ))
                    .filter(GetResult::isExists);
            });
        return InMemoryBatchIterator.of(getResultStream::iterator, null);
    }
}
