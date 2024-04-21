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

package io.crate.execution.engine.indexing;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.jetbrains.annotations.Nullable;

import io.crate.common.collections.Maps;
import io.crate.data.BatchIterator;
import io.crate.data.CollectingBatchIterator;
import io.crate.data.Input;
import io.crate.data.Projector;
import io.crate.data.Row;
import io.crate.data.breaker.RamAccounting;
import io.crate.execution.dml.upsert.ShardUpsertRequest;
import io.crate.execution.dml.upsert.ShardUpsertRequest.DuplicateKeyAction;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.execution.engine.collect.RowShardResolver;
import io.crate.execution.jobs.NodeLimits;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.TransactionContext;

public class IndexWriterProjector implements Projector {

    private final ShardingUpsertExecutor shardingUpsertExecutor;

    public IndexWriterProjector(ClusterService clusterService,
                                NodeLimits nodeJobsCounter,
                                CircuitBreaker queryCircuitBreaker,
                                RamAccounting ramAccounting,
                                ScheduledExecutorService scheduler,
                                Executor executor,
                                TransactionContext txnCtx,
                                NodeContext nodeCtx,
                                Settings settings,
                                int targetTableNumShards,
                                int targetTableNumReplicas,
                                ElasticsearchClient elasticsearchClient,
                                Supplier<String> indexNameResolver,
                                Reference rawSourceReference,
                                List<ColumnIdent> primaryKeyIdents,
                                List<? extends Symbol> primaryKeySymbols,
                                @Nullable Symbol routingSymbol,
                                ColumnIdent clusteredByColumn,
                                Input<?> sourceInput,
                                List<? extends CollectExpression<Row, ?>> collectExpressions,
                                int bulkActions,
                                @Nullable String[] excludes,
                                boolean autoCreateIndices,
                                boolean overwriteDuplicates,
                                UUID jobId,
                                UpsertResultContext upsertResultContext,
                                boolean failFast) {
        Input<String> source;
        if (excludes == null) {
            //noinspection unchecked
            source = (Input<String>) sourceInput;
        } else {
            //noinspection unchecked
            source = new MapInput((Input<Map<String, Object>>) sourceInput, excludes);
        }
        RowShardResolver rowShardResolver = new RowShardResolver(
            txnCtx, nodeCtx, primaryKeyIdents, primaryKeySymbols, clusteredByColumn, routingSymbol);
        ShardUpsertRequest.Builder builder = new ShardUpsertRequest.Builder(
            txnCtx.sessionSettings(),
            ShardingUpsertExecutor.BULK_REQUEST_TIMEOUT_SETTING.get(settings),
            overwriteDuplicates ? DuplicateKeyAction.OVERWRITE : DuplicateKeyAction.UPDATE_OR_FAIL,
            true,
            null,
            new Reference[]{rawSourceReference},
            null,
            jobId
        );

        ItemFactory<ShardUpsertRequest.Item> itemFactory = (id, pkValues, autoGeneratedTimestamp) -> ShardUpsertRequest.Item.forInsert(
            id,
            pkValues,
            autoGeneratedTimestamp,
            new Object[]{source.value()},
            null
        );

        Predicate<UpsertResults> earlyTerminationCondition = results -> failFast && results.containsErrors();
        Function<UpsertResults, Throwable> earlyTerminationExceptionGenerator = UpsertResults::resultsToFailure;
        shardingUpsertExecutor = new ShardingUpsertExecutor(
            clusterService,
            (ignored1, ignored2) -> {},
            nodeJobsCounter,
            queryCircuitBreaker,
            ramAccounting,
            scheduler,
            executor,
            bulkActions,
            jobId,
            rowShardResolver,
            itemFactory,
            builder::newRequest,
            collectExpressions,
            indexNameResolver,
            autoCreateIndices,
            elasticsearchClient,
            targetTableNumShards,
            targetTableNumReplicas,
            upsertResultContext,
            earlyTerminationCondition,
            earlyTerminationExceptionGenerator
        );
    }


    @Override
    public BatchIterator<Row> apply(BatchIterator<Row> batchIterator) {
        return CollectingBatchIterator.newInstance(batchIterator, shardingUpsertExecutor, batchIterator.hasLazyResultSet());
    }

    @Override
    public boolean providesIndependentScroll() {
        return false;
    }

    private static class MapInput implements Input<String> {

        private final Input<Map<String, Object>> sourceInput;
        private final String[] excludes;
        private static final Logger LOGGER = LogManager.getLogger(MapInput.class);
        private int lastSourceSize;

        private MapInput(Input<Map<String, Object>> sourceInput, String[] excludes) {
            this.sourceInput = sourceInput;
            this.excludes = excludes;
            this.lastSourceSize = PageCacheRecycler.BYTE_PAGE_SIZE;
        }

        @Override
        public String value() {
            Map<String, Object> value = sourceInput.value();
            if (value == null) {
                return null;
            }
            assert value instanceof LinkedHashMap<String, Object> : "the raw source order should be preserved";
            if (excludes != null) {
                for (String exclude : excludes) {
                    String[] path = exclude.split("\\.");
                    Maps.removeByPath(value, Arrays.asList(path));
                }
            }
            try (XContentBuilder xContentBuilder = new XContentBuilder(XContentType.JSON.xContent(), new BytesStreamOutput(lastSourceSize))) {
                BytesReference bytes = BytesReference.bytes(xContentBuilder.map(value));
                lastSourceSize = bytes.length();
                return bytes.utf8ToString();
            } catch (IOException ex) {
                LOGGER.error("could not parse xContent", ex);
            }
            return null;
        }
    }
}

