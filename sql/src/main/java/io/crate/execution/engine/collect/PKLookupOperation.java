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

package io.crate.execution.engine.collect;

import io.crate.breaker.RamAccounting;
import io.crate.data.BatchIterator;
import io.crate.data.CompositeBatchIterator;
import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Row;
import io.crate.data.RowN;
import io.crate.data.SentinelRow;
import io.crate.execution.dsl.projection.Projection;
import io.crate.execution.engine.collect.sources.ShardCollectSource;
import io.crate.execution.engine.pipeline.ProjectorFactory;
import io.crate.execution.engine.pipeline.Projectors;
import io.crate.expression.reference.Doc;
import io.crate.expression.reference.doc.lucene.SourceFieldVisitor;
import io.crate.memory.MemoryManager;
import io.crate.metadata.TransactionContext;
import io.crate.planner.operators.PKAndVersion;
import org.apache.lucene.index.Term;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.indices.IndicesService;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class PKLookupOperation {

    private final IndicesService indicesService;
    private final ShardCollectSource shardCollectSource;

    public PKLookupOperation(IndicesService indicesService, ShardCollectSource shardCollectSource) {
        this.indicesService = indicesService;
        this.shardCollectSource = shardCollectSource;
    }

    @Nullable
    public static Doc lookupDoc(IndexShard shard, String id, long version, VersionType versionType, long seqNo, long primaryTerm) {
        Term uidTerm = new Term(IdFieldMapper.NAME, Uid.encodeId(id));
        Engine.Get get = new Engine.Get(true, true, id, uidTerm)
            .version(version)
            .versionType(versionType)
            .setIfSeqNo(seqNo)
            .setIfPrimaryTerm(primaryTerm);

        try (Engine.GetResult getResult = shard.get(get)) {
            var docIdAndVersion = getResult.docIdAndVersion();
            if (docIdAndVersion == null) {
                return null;
            }
            SourceFieldVisitor visitor = new SourceFieldVisitor();
            try {
                docIdAndVersion.reader.document(docIdAndVersion.docId, visitor);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return new Doc(
                docIdAndVersion.docId,
                shard.shardId().getIndexName(),
                id,
                docIdAndVersion.version,
                docIdAndVersion.seqNo,
                docIdAndVersion.primaryTerm,
                XContentHelper.toMap(visitor.source(), XContentType.JSON),
                () -> visitor.source().utf8ToString()
            );
        }
    }

    public BatchIterator<Row> lookup(UUID jobId,
                                     TransactionContext txnCtx,
                                     Supplier<RamAccounting> ramAccountingSupplier,
                                     Supplier<MemoryManager> memoryManagerSupplier,
                                     boolean ignoreMissing,
                                     Map<ShardId, List<PKAndVersion>> idsByShard,
                                     Collection<? extends Projection> projections,
                                     boolean requiresScroll,
                                     Function<Doc, Row> resultToRow) {
        ArrayList<BatchIterator<Row>> iterators = new ArrayList<>(idsByShard.size());
        for (Map.Entry<ShardId, List<PKAndVersion>> idsByShardEntry : idsByShard.entrySet()) {
            ShardId shardId = idsByShardEntry.getKey();
            IndexService indexService = indicesService.indexService(shardId.getIndex());
            if (indexService == null) {
                if (ignoreMissing) {
                    continue;
                }
                throw new IndexNotFoundException(shardId.getIndex());
            }
            IndexShard shard = indexService.getShardOrNull(shardId.id());
            if (shard == null) {
                if (ignoreMissing) {
                    continue;
                }
                throw new ShardNotFoundException(shardId);
            }
            Stream<Row> rowStream = idsByShardEntry.getValue().stream()
                .map(pkAndVersion -> lookupDoc(
                    shard,
                    pkAndVersion.id(),
                    pkAndVersion.version(),
                    VersionType.EXTERNAL,
                    pkAndVersion.seqNo(),
                    pkAndVersion.primaryTerm()))
                .filter(Objects::nonNull)
                .map(resultToRow);

            if (projections.isEmpty()) {
                final Iterable<Row> rowIterable = requiresScroll
                    ? rowStream.map(row -> new RowN(row.materialize())).collect(Collectors.toList())
                    : rowStream::iterator;
                iterators.add(InMemoryBatchIterator.of(rowIterable, SentinelRow.SENTINEL, true));
            } else {
                ProjectorFactory projectorFactory;
                try {
                    projectorFactory = shardCollectSource.getProjectorFactory(shardId);
                } catch (ShardNotFoundException e) {
                    if (ignoreMissing) {
                        continue;
                    }
                    throw e;
                }
                Projectors projectors = new Projectors(
                    projections,
                    jobId,
                    txnCtx,
                    ramAccountingSupplier.get(),
                    memoryManagerSupplier.get(),
                    projectorFactory);
                final Iterable<Row> rowIterable = requiresScroll && !projectors.providesIndependentScroll()
                    ? rowStream.map(row -> new RowN(row.materialize())).collect(Collectors.toList())
                    : rowStream::iterator;
                iterators.add(projectors.wrap(InMemoryBatchIterator.of(rowIterable, SentinelRow.SENTINEL, true)));
            }
        }
        //noinspection unchecked
        return CompositeBatchIterator.seqComposite(iterators.toArray(new BatchIterator[0]));
    }
}
