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

package io.crate.execution.engine.indexing;

import io.crate.data.BatchIterator;
import io.crate.data.CollectingBatchIterator;
import io.crate.data.Input;
import io.crate.data.Projector;
import io.crate.data.Row;
import io.crate.execution.TransportActionProvider;
import io.crate.execution.dml.upsert.ShardUpsertRequest;
import io.crate.execution.dml.upsert.ShardUpsertRequest.DuplicateKeyAction;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.execution.engine.collect.RowShardResolver;
import io.crate.execution.jobs.NodeJobsCounter;
import io.crate.expression.InputRow;
import io.crate.expression.symbol.Assignments;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Functions;
import io.crate.metadata.Reference;
import io.crate.metadata.TransactionContext;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import java.util.function.Supplier;

public class ColumnIndexWriterProjector implements Projector {

    private final ShardingUpsertExecutor shardingUpsertExecutor;

    public ColumnIndexWriterProjector(ClusterService clusterService,
                                      NodeJobsCounter nodeJobsCounter,
                                      ScheduledExecutorService scheduler,
                                      Executor executor,
                                      TransactionContext txnCtx,
                                      Functions functions,
                                      Settings settings,
                                      int targetTableNumShards,
                                      int targetTableNumReplicas,
                                      Supplier<String> indexNameResolver,
                                      TransportActionProvider transportActionProvider,
                                      List<ColumnIdent> primaryKeyIdents,
                                      List<? extends Symbol> primaryKeySymbols,
                                      @Nullable Symbol routingSymbol,
                                      ColumnIdent clusteredByColumn,
                                      List<Reference> columnReferences,
                                      List<Input<?>> insertInputs,
                                      List<? extends CollectExpression<Row, ?>> collectExpressions,
                                      boolean ignoreDuplicateKeys,
                                      @Nullable Map<Reference, Symbol> updateAssignments,
                                      int bulkActions,
                                      boolean autoCreateIndices,
                                      UUID jobId) {
        RowShardResolver rowShardResolver = new RowShardResolver(
            txnCtx, functions, primaryKeyIdents, primaryKeySymbols, clusteredByColumn, routingSymbol);
        assert columnReferences.size() == insertInputs.size()
            : "number of insert inputs must be equal to the number of columns";

        String[] updateColumnNames;
        Symbol[] assignments;
        if (updateAssignments == null) {
            updateColumnNames = null;
            assignments = null;
        } else {
            Assignments convert = Assignments.convert(updateAssignments);
            updateColumnNames = convert.targetNames();
            assignments = convert.sources();
        }
        ShardUpsertRequest.Builder builder = new ShardUpsertRequest.Builder(
            txnCtx.sessionSettings(),
            ShardingUpsertExecutor.BULK_REQUEST_TIMEOUT_SETTING.setting().get(settings),
            ignoreDuplicateKeys ? DuplicateKeyAction.IGNORE : DuplicateKeyAction.UPDATE_OR_FAIL,
            true, // continueOnErrors
            updateColumnNames,
            columnReferences.toArray(new Reference[columnReferences.size()]),
            jobId,
            true
        );

        InputRow insertValues = new InputRow(insertInputs);
        Function<String, ShardUpsertRequest.Item> itemFactory = id -> new ShardUpsertRequest.Item(
            id, assignments, insertValues.materialize(), null, null, null);

        shardingUpsertExecutor = new ShardingUpsertExecutor(
            clusterService,
            nodeJobsCounter,
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
            transportActionProvider.transportShardUpsertAction()::execute,
            transportActionProvider.transportBulkCreateIndicesAction(),
            targetTableNumShards,
            targetTableNumReplicas,
            UpsertResultContext.forRowCount()
        );
    }

    @Override
    public BatchIterator<Row> apply(BatchIterator<Row> batchIterator) {
        return CollectingBatchIterator.newInstance(batchIterator, shardingUpsertExecutor, batchIterator.involvesIO());
    }

    @Override
    public boolean providesIndependentScroll() {
        return false;
    }
}
