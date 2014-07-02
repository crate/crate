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

package io.crate.operation.projectors;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import io.crate.PartitionName;
import io.crate.analyze.Id;
import io.crate.metadata.ColumnIdent;
import io.crate.operation.Input;
import io.crate.operation.ProjectorUpstream;
import io.crate.operation.collect.CollectExpression;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.bulk.BulkShardProcessor;
import org.elasticsearch.action.bulk.TransportShardBulkAction;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.threadpool.ThreadPool;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class IndexWriterProjector implements Projector {

    private final AtomicInteger remainingUpstreams = new AtomicInteger(0);
    private final CollectExpression<?>[] collectExpressions;
    private final List<Input<?>> idInputs;
    private final Input<?> sourceInput;
    private final Input<?> routingInput;
    private final String tableName;
    private final Object lock = new Object();
    private final List<ColumnIdent> primaryKeys;
    private final List<Input<?>> partitionedByInputs;
    private final String[] includes;
    private final String[] excludes;
    private final BulkShardProcessor bulkShardProcessor;
    private Projector downstream;
    private final Function<Input<?>, BytesRef> inputToBytesRef = new Function<Input<?>, BytesRef>() {
        @Nullable
        @Override
        public BytesRef apply(Input<?> input) {
            return BytesRefs.toBytesRef(input.value());
        }
    };

    public IndexWriterProjector(ThreadPool threadPool,
                                ClusterService clusterService,
                                Settings settings,
                                TransportShardBulkAction transportShardBulkAction,
                                TransportCreateIndexAction transportCreateIndexAction,
                                String tableName,
                                List<ColumnIdent> primaryKeys,
                                List<Input<?>> idInputs,
                                List<Input<?>> partitionedByInputs,
                                Input<?> routingInput,
                                Input<?> sourceInput,
                                CollectExpression<?>[] collectExpressions,
                                @Nullable Integer bulkActions,
                                @Nullable String[] includes,
                                @Nullable String[] excludes) {
        this.tableName = tableName;
        this.primaryKeys = primaryKeys;
        this.collectExpressions = collectExpressions;
        this.idInputs = idInputs;
        this.routingInput = routingInput;
        this.sourceInput = sourceInput;
        this.partitionedByInputs = partitionedByInputs;
        this.includes = includes;
        this.excludes = excludes;
        this.bulkShardProcessor = new BulkShardProcessor(
                threadPool,
                clusterService,
                settings,
                transportShardBulkAction,
                transportCreateIndexAction,
                partitionedByInputs.size() > 0, // autoCreate indices if this is a partitioned table
                Objects.firstNonNull(bulkActions, 100)
        );
    }

    @Override
    public void startProjection() {
    }

    @Override
    public boolean setNextRow(Object... row) {
        String indexName;
        BytesReference source;
        String id;
        String clusteredByValue;

        synchronized (lock) {
            for (CollectExpression<?> collectExpression : collectExpressions) {
                collectExpression.setNextRow(row);
            }

            Object value = sourceInput.value();
            if (value == null) {
                return true;
            }
            clusteredByValue = BytesRefs.toString(routingInput.value());
            id = getId(clusteredByValue).stringValue();
            source = getSource(value);
            indexName = getIndexName();
        }

        return bulkShardProcessor.add(indexName, source, id, clusteredByValue);
    }

    private BytesReference getSource(Object value) {
        if (includes != null || excludes != null) {
            assert value instanceof Map;
            // exclude partitioned columns from source
            Map<String, Object> sourceAsMap = XContentMapValues.filter((Map) value, includes, excludes);

            try {
                return XContentFactory.contentBuilder(Requests.INDEX_CONTENT_TYPE).map(sourceAsMap).bytes();
            } catch (IOException e) {
                // TODO: handle correctly
                return null;
            }
        } else {
            assert value instanceof BytesRef;
            byte[] bytes = ((BytesRef) value).bytes;
            return new BytesArray(bytes, 0, bytes.length);
        }
    }

    public Id getId(String clusteredByValue) {
        return new Id(
                primaryKeys,
                Lists.transform(idInputs, inputToBytesRef),
                ColumnIdent.fromPath(clusteredByValue),
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
        Futures.addCallback(bulkShardProcessor.result(), new FutureCallback<Long>() {
            @Override
            public void onSuccess(@Nullable Long result) {
                downstream.setNextRow(result);
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
            return new PartitionName(tableName,
                    Lists.transform(partitionedByInputs, inputToBytesRef)).stringValue();
        } else {
            return tableName;
        }
    }

    @Override
    public void downstream(Projector downstream) {
        this.downstream = downstream;
        setResultCallback();
    }

    @Override
    public Projector downstream() {
        return downstream;
    }
}

