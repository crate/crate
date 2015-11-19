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

import com.google.common.base.MoreObjects;
import com.google.common.base.Supplier;
import com.google.common.util.concurrent.Futures;
import io.crate.analyze.symbol.Reference;
import io.crate.analyze.symbol.Symbol;
import io.crate.core.collections.Row;
import io.crate.executor.transport.ShardUpsertRequest;
import io.crate.executor.transport.TransportActionProvider;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.settings.CrateSettings;
import io.crate.operation.collect.CollectExpression;
import io.crate.operation.collect.RowShardResolver;
import org.elasticsearch.action.admin.indices.create.TransportBulkCreateIndicesAction;
import org.elasticsearch.action.bulk.AssignmentVisitor;
import org.elasticsearch.action.bulk.BulkRetryCoordinatorPool;
import org.elasticsearch.action.bulk.BulkShardProcessor;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.settings.Settings;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;

public class ColumnIndexWriterProjector extends AbstractProjector {

    private final Iterable<? extends CollectExpression<Row, ?>> collectExpressions;

    private final RowShardResolver rowShardResolver;
    private final Supplier<String> indexNameResolver;
    private final BulkRetryCoordinatorPool bulkRetryCoordinatorPool;
    private final TransportActionProvider transportActionProvider;
    private BulkShardProcessor bulkShardProcessor;
    private final AtomicBoolean failed = new AtomicBoolean(false);


    /**
     * 3 states:
     *
     * <ul>
     * <li> writing into a normal table - <code>partitionIdent = null, partitionedByInputs = []</code></li>
     * <li> writing into a partitioned table - <code>partitionIdent = null, partitionedByInputs = [...]</code></li>
     * <li> writing into a single partition - <code>partitionIdent = "...", partitionedByInputs = []</code></li>
     * </ul>
     */
    protected ColumnIndexWriterProjector(ClusterService clusterService,
                                         Settings settings,
                                         Supplier<String> indexNameResolver,
                                         TransportActionProvider transportActionProvider,
                                         BulkRetryCoordinatorPool bulkRetryCoordinatorPool,
                                         List<ColumnIdent> primaryKeyIdents,
                                         List<Symbol> primaryKeySymbols,
                                         @Nullable Symbol routingSymbol,
                                         ColumnIdent clusteredByColumn,
                                         List<Reference> columnReferences,
                                         List<Symbol> columnSymbols,
                                         Iterable<? extends CollectExpression<Row, ?>> collectExpressions,
                                         @Nullable
                                           Map<Reference, Symbol> updateAssignments,
                                         @Nullable Integer bulkActions,
                                         boolean autoCreateIndices,
                                         UUID jobId) {
        this.bulkRetryCoordinatorPool = bulkRetryCoordinatorPool;
        this.transportActionProvider = transportActionProvider;
        this.indexNameResolver = indexNameResolver;
        this.collectExpressions = collectExpressions;
        rowShardResolver = new RowShardResolver(primaryKeyIdents, primaryKeySymbols, clusteredByColumn, routingSymbol);
        assert columnReferences.size() == columnSymbols.size();

        Map<Reference, Symbol> insertAssignments = new HashMap<>(columnReferences.size());
        for (int i = 0; i < columnReferences.size(); i++) {
            insertAssignments.put(columnReferences.get(i), columnSymbols.get(i));
        }
        createBulkShardProcessor(
                clusterService,
                settings,
                transportActionProvider.transportBulkCreateIndicesAction(),
                bulkActions,
                autoCreateIndices,
                false, // overwriteDuplicates
                updateAssignments,
                insertAssignments,
                jobId);
    }

    protected void createBulkShardProcessor(ClusterService clusterService,
                                            Settings settings,
                                            TransportBulkCreateIndicesAction transportBulkCreateIndicesAction,
                                            @Nullable Integer bulkActions,
                                            boolean autoCreateIndices,
                                            boolean overwriteDuplicates,
                                            @Nullable Map<Reference, Symbol> updateAssignments,
                                            @Nullable Map<Reference, Symbol> insertAssignments,
                                            UUID jobId) {

        AssignmentVisitor.AssignmentVisitorContext visitorContext = AssignmentVisitor.processAssignments(
                updateAssignments,
                insertAssignments
        );
        ShardUpsertRequest.Builder requestBuilder = new ShardUpsertRequest.Builder(
                visitorContext.dataTypes(),
                visitorContext.columnIndicesToStream(),
                CrateSettings.BULK_REQUEST_TIMEOUT.extractTimeValue(settings),
                true,
                overwriteDuplicates,
                updateAssignments,
                insertAssignments,
                null,
                jobId
        );
        bulkShardProcessor = new BulkShardProcessor(
                clusterService,
                transportBulkCreateIndicesAction,
                rowShardResolver,
                autoCreateIndices,
                MoreObjects.firstNonNull(bulkActions, 100),
                bulkRetryCoordinatorPool,
                requestBuilder,
                transportActionProvider.transportShardUpsertActionDelegate(),
                jobId);
    }

    @Override
    public void downstream(RowReceiver rowReceiver) {
        super.downstream(rowReceiver);
        Futures.addCallback(bulkShardProcessor.result(), new BulkProcessorFutureCallback(failed, rowReceiver));
    }

    @Override
    public boolean setNextRow(Row row) {
        for (CollectExpression<Row, ?> collectExpression : collectExpressions) {
            collectExpression.setNextRow(row);
        }
        return bulkShardProcessor.add(indexNameResolver.get(), row, null);
    }

    @Override
    public void finish() {
        bulkShardProcessor.close();
    }

    @Override
    public void fail(Throwable throwable) {
        failed.set(true);
        downstream.fail(throwable);
        if (throwable instanceof CancellationException) {
            bulkShardProcessor.kill();
        } else {
            bulkShardProcessor.close();
        }
    }
}
