/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.operation.projectors;

import com.google.common.base.Function;
import com.google.common.base.MoreObjects;
import com.google.common.base.Optional;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import io.crate.analyze.Id;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.PartitionName;
import io.crate.operation.Input;
import io.crate.operation.ProjectorUpstream;
import io.crate.operation.collect.CollectExpression;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.bulk.BulkShardProcessor;
import org.elasticsearch.action.bulk.TransportShardUpsertActionDelegate;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.settings.Settings;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class AbstractIndexWriterProjector implements Projector {

    private static final Symbol[] EMPTY_ASSIGNMENTS = new Symbol[0];

    private final AtomicInteger remainingUpstreams = new AtomicInteger(0);
    private final CollectExpression<?>[] collectExpressions;
    private final List<Input<?>> idInputs;
    private final Optional<ColumnIdent> routingIdent;
    private final Optional<Input<?>> routingInput;
    private final String tableName;
    private final Object lock = new Object();
    private final List<ColumnIdent> primaryKeys;
    private final List<Input<?>> partitionedByInputs;
    private final Function<Input<?>, BytesRef> inputToBytesRef = new Function<Input<?>, BytesRef>() {
                @Nullable
                @Override
                public BytesRef apply(Input<?> input) {
                    return BytesRefs.toBytesRef(input.value());
                }
            };
    private BulkShardProcessor bulkShardProcessor;
    private Projector downstream;

    private final LoadingCache<List<BytesRef>, String> partitionIdentCache;

    protected AbstractIndexWriterProjector(final String tableName,
                                           List<ColumnIdent> primaryKeys,
                                           List<Input<?>> idInputs,
                                           List<Input<?>> partitionedByInputs,
                                           @Nullable ColumnIdent clusteredBy,
                                           @Nullable Input<?> routingInput,
                                           CollectExpression<?>[] collectExpressions) {
        this.tableName = tableName;
        this.primaryKeys = primaryKeys;
        this.collectExpressions = collectExpressions;
        this.idInputs = idInputs;
        this.routingIdent = Optional.fromNullable(clusteredBy);
        this.routingInput = Optional.<Input<?>>fromNullable(routingInput);
        this.partitionedByInputs = partitionedByInputs;
        if (partitionedByInputs.size() > 0) {
            partitionIdentCache = CacheBuilder.newBuilder()
                        .initialCapacity(10)
                        .maximumSize(20)
                        .build(new CacheLoader<List<BytesRef>, String>() {
                            @Override
                            public String load(@Nonnull List<BytesRef> key) throws Exception {
                                return new PartitionName(tableName, key).stringValue();
                            }
                        });
        } else {
            partitionIdentCache = null;
        }
    }

    protected void createBulkShardProcessor(ClusterService clusterService,
                                            Settings settings,
                                            TransportShardUpsertActionDelegate transportShardUpsertActionDelegate,
                                            TransportCreateIndexAction transportCreateIndexAction,
                                            @Nullable Integer bulkActions,
                                            boolean autoCreateIndices,
                                            boolean overwriteDuplicates,
                                            @Nullable String[] assignmentsColumns,
                                            @Nullable Reference[] missingAssignmentsColumns) {
        bulkShardProcessor = new BulkShardProcessor(
                clusterService,
                settings,
                transportShardUpsertActionDelegate,
                transportCreateIndexAction,
                autoCreateIndices,
                overwriteDuplicates,
                MoreObjects.firstNonNull(bulkActions, 100),
                true,
                assignmentsColumns,
                missingAssignmentsColumns
        );
    }

    protected Symbol[] generateAssignments() {
        return EMPTY_ASSIGNMENTS;
    }

    protected abstract Object[] generateMissingAssignments();

    @Override
    public void startProjection() {
        assert bulkShardProcessor != null : "must create a BulkShardProcessor first";
    }

    @Override
    public boolean setNextRow(Object... row) {
        String indexName;
        Symbol[] assignments;
        Object[] missingAssignments;
        String id;
        String clusteredByValue = null;

        synchronized (lock) {
            for (CollectExpression<?> collectExpression : collectExpressions) {
                collectExpression.setNextRow(row);
            }

            assignments = generateAssignments();
            missingAssignments = generateMissingAssignments();
            if (missingAssignments == null && assignments == null) {
                return true;
            }
            if (routingInput.isPresent()) {
                clusteredByValue = BytesRefs.toString(routingInput.get().value());
            }
            id = getId().stringValue();
            indexName = getIndexName();
        }

        if (id == null) {
            throw new IllegalArgumentException("Primary key value must not be NULL");
        }
        return bulkShardProcessor.add(indexName, id, assignments, missingAssignments, clusteredByValue, null);
    }

    public Id getId() {
        return new Id(
                primaryKeys,
                Lists.transform(idInputs, inputToBytesRef),
                routingIdent.orNull(),
                true
        );
    }

    @Override
    public void registerUpstream(ProjectorUpstream upstream) {
        remainingUpstreams.incrementAndGet();
    }

    @Override
    public void upstreamFinished() {
        if (remainingUpstreams.decrementAndGet() <= 0) {
            bulkShardProcessor.close();
        }
    }

    @Override
    public void upstreamFailed(Throwable throwable) {
        if (downstream != null) {
            downstream.upstreamFailed(throwable);
        }
        bulkShardProcessor.close();
    }

    private void setResultCallback() {
        assert downstream != null;
        Futures.addCallback(bulkShardProcessor.result(), new FutureCallback<BitSet>() {
            @Override
            public void onSuccess(@Nullable BitSet result) {
                long rowCount = result == null ? 0L : result.cardinality();
                downstream.setNextRow(rowCount);
                downstream.upstreamFinished();
            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
                downstream.upstreamFailed(t);
            }
        });
    }

    private String getIndexName() {
        if (partitionedByInputs.size() > 0) {
            List<BytesRef> partitions = Lists.transform(partitionedByInputs, inputToBytesRef);
            try {
                return partitionIdentCache.get(partitions);
            } catch (ExecutionException e) {
                throw ExceptionsHelper.convertToRuntime(e);
            }
        } else {
            return tableName;
        }
    }

    @Override
    public void downstream(Projector downstream) {
        downstream.registerUpstream(this);
        this.downstream = downstream;
        setResultCallback();
    }
}
