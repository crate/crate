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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.jetbrains.annotations.Nullable;

import io.crate.common.unit.TimeValue;
import io.crate.data.BatchIterator;
import io.crate.data.CollectingBatchIterator;
import io.crate.data.Input;
import io.crate.data.Projector;
import io.crate.data.Row;
import io.crate.data.breaker.RamAccounting;
import io.crate.exceptions.ConversionException;
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
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.settings.SessionSettings;

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
                                DocTableInfo table,
                                Settings settings,
                                int targetTableNumShards,
                                int targetTableNumReplicas,
                                ElasticsearchClient elasticsearchClient,
                                Supplier<String> indexNameResolver,
                                Reference docReference,
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

        // TODO: if the request came from a earlier version, this might still be _raw
        assert docReference.column().equals(DocSysColumns.DOC)
            : "IndexWriterProjector supports only _doc reference";

        Input<Map<String, Object>> docInput = (Input<Map<String, Object>>) sourceInput;
        RowShardResolver rowShardResolver = new RowShardResolver(
            txnCtx, nodeCtx, primaryKeyIdents, primaryKeySymbols, clusteredByColumn, routingSymbol);

        TimeValue timeout = ShardingUpsertExecutor.BULK_REQUEST_TIMEOUT_SETTING.get(settings);
        boolean continueOnError = true;
        DuplicateKeyAction duplicateKeyAction = overwriteDuplicates
            ? DuplicateKeyAction.OVERWRITE
            : DuplicateKeyAction.UPDATE_OR_FAIL;
        String[] updateColumns = null;
        Symbol[] returnValues = null;
        ShardingUpsertExecutor.BULK_REQUEST_TIMEOUT_SETTING.get(settings);
        final AtomicReference<Reference[]> targetColumns = new AtomicReference<>();
        final SessionSettings sessionSettings = txnCtx.sessionSettings();
        Function<ShardId, ShardUpsertRequest> createRequest = shardId -> {
            Reference[] insertColumns = targetColumns.get();
            assert insertColumns != null : "targetColumns must be set";
            return new ShardUpsertRequest(
                shardId,
                jobId,
                continueOnError,
                duplicateKeyAction,
                sessionSettings,
                updateColumns,
                insertColumns,
                returnValues
            ).timeout(timeout);
        };
        ItemFactory<ShardUpsertRequest.Item> itemFactory = (id, pkValues, autoGeneratedTimestamp) -> {
            Map<String, Object> doc = docInput.value();
            // TODO: ensure target columns are not changing?
            // Or if, force new request?
            Set<String> keys = doc.keySet();
            Reference[] references = new Reference[keys.size()];
            int i = 0;
            for (String key : doc.keySet()) {
                ColumnIdent column = ColumnIdent.fromNameSafe(key, List.of());
                Reference reference = table.getReference(column);
                if (reference == null) {
                    reference = table.getDynamic(column, true, sessionSettings.errorOnUnknownObjectKey());
                }
                references[i] = reference;
                i++;
            }
            Reference[] prevReferences = targetColumns.get();
            targetColumns.set(references);
            if (prevReferences != null && !Arrays.deepEquals(prevReferences, references)) {
                throw new IllegalArgumentException("All lines must contain the same columns in COPY FROM");
            }
            try {
                Object[] values = new Object[references.length];
                for (int j = 0; j < references.length; j++) {
                    Reference reference = references[j];
                    Object value = doc.get(reference.column().name());
                    try {
                        values[j] = reference.valueType().implicitCast(value);
                    } catch (ConversionException e) {
                        throw e;
                    } catch (ClassCastException | IllegalArgumentException e) {
                        throw new ConversionException(value, reference.valueType());
                    }
                }
                return ShardUpsertRequest.Item.forInsert(
                    id,
                    pkValues,
                    autoGeneratedTimestamp,
                    values,
                    null
                );
            } catch (Throwable t) {
                throw t;
            }
        };

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
            createRequest,
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
        return CollectingBatchIterator.newInstance(
            batchIterator, shardingUpsertExecutor, batchIterator.hasLazyResultSet());
    }

    @Override
    public boolean providesIndependentScroll() {
        return false;
    }
}
