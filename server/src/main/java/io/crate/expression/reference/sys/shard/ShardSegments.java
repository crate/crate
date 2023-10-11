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

package io.crate.expression.reference.sys.shard;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.index.engine.Segment;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;

@Singleton
public class ShardSegments implements Iterable<ShardSegment> {

    private final IndicesService indicesService;

    @Inject
    public ShardSegments(IndicesService indicesService) {
        this.indicesService = indicesService;
    }

    @Override
    public Iterator<ShardSegment> iterator() {
        return StreamSupport.stream(indicesService.spliterator(), false)
            .flatMap(indexService -> StreamSupport.stream(indexService.spliterator(), false))
            .filter(x -> !x.routingEntry().unassigned())
            .flatMap(this::buildShardSegment)
            .iterator();
    }

    private Stream<ShardSegment> buildShardSegment(IndexShard indexShard) {
        try {
            List<Segment> segments = indexShard.segments(false);
            ShardId shardId = indexShard.shardId();
            return segments.stream().map(
                sgmt -> new ShardSegment(shardId.id(),
                                         shardId.getIndexName(),
                                         sgmt,
                                         indexShard.routingEntry().primary()));
        } catch (AlreadyClosedException ignored) {
            return Stream.empty();
        }
    }
}
