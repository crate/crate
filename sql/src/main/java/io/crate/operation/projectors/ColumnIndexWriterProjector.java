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
import io.crate.analyze.symbol.Assignments;
import io.crate.analyze.symbol.Symbol;
import io.crate.data.BatchIterator;
import io.crate.data.Input;
import io.crate.data.Projector;
import io.crate.data.Row;
import io.crate.executor.transport.ShardUpsertRequest;
import io.crate.executor.transport.TransportActionProvider;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Functions;
import io.crate.metadata.Reference;
import io.crate.metadata.settings.CrateSettings;
import io.crate.operation.InputRow;
import io.crate.operation.collect.CollectExpression;
import io.crate.operation.collect.RowShardResolver;
import org.elasticsearch.action.bulk.BulkRetryCoordinatorPool;
import org.elasticsearch.action.bulk.BulkShardProcessor;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;

public class ColumnIndexWriterProjector implements Projector {

    private final Iterable<? extends CollectExpression<Row, ?>> collectExpressions;

    private final RowShardResolver rowShardResolver;
    private final Supplier<String> indexNameResolver;
    private final Symbol[] assignments;
    private final InputRow insertValues;
    private BulkShardProcessor<ShardUpsertRequest> bulkShardProcessor;

    protected ColumnIndexWriterProjector(ClusterService clusterService,
                                         Functions functions,
                                         IndexNameExpressionResolver indexNameExpressionResolver,
                                         Settings settings,
                                         Supplier<String> indexNameResolver,
                                         TransportActionProvider transportActionProvider,
                                         BulkRetryCoordinatorPool bulkRetryCoordinatorPool,
                                         List<ColumnIdent> primaryKeyIdents,
                                         List<? extends Symbol> primaryKeySymbols,
                                         @Nullable Symbol routingSymbol,
                                         ColumnIdent clusteredByColumn,
                                         List<Reference> columnReferences,
                                         List<Input<?>> insertInputs,
                                         Iterable<? extends CollectExpression<Row, ?>> collectExpressions,
                                         @Nullable Map<Reference, Symbol> updateAssignments,
                                         @Nullable Integer bulkActions,
                                         boolean autoCreateIndices,
                                         UUID jobId) {
        this.indexNameResolver = indexNameResolver;
        this.collectExpressions = collectExpressions;
        rowShardResolver = new RowShardResolver(functions, primaryKeyIdents, primaryKeySymbols, clusteredByColumn, routingSymbol);
        assert columnReferences.size() == insertInputs.size()
            : "number of insert inputs must be equal to the number of columns";

        String[] updateColumnNames;
        if (updateAssignments == null) {
            updateColumnNames = null;
            assignments = null;
        } else {
            Tuple<String[], Symbol[]> convert = Assignments.convert(updateAssignments);
            updateColumnNames = convert.v1();
            assignments = convert.v2();
        }
        ShardUpsertRequest.Builder builder = new ShardUpsertRequest.Builder(
            CrateSettings.BULK_REQUEST_TIMEOUT.extractTimeValue(settings),
            false, // overwriteDuplicates
            true, // continueOnErrors
            updateColumnNames,
            columnReferences.toArray(new Reference[columnReferences.size()]),
            jobId);

        insertValues = new InputRow(insertInputs);
        bulkShardProcessor = new BulkShardProcessor<>(
            clusterService,
            transportActionProvider.transportBulkCreateIndicesAction(),
            indexNameExpressionResolver,
            settings,
            bulkRetryCoordinatorPool,
            autoCreateIndices,
            MoreObjects.firstNonNull(bulkActions, 100),
            builder,
            transportActionProvider.transportShardUpsertAction()::execute,
            jobId
        );
    }

    @Override
    public BatchIterator apply(BatchIterator batchIterator) {
        Supplier<ShardUpsertRequest.Item> updateItemSupplier = () -> new ShardUpsertRequest.Item(
            rowShardResolver.id(), assignments, insertValues.materialize(), null);
        return IndexWriterCountBatchIterator.newIndexInstance(
            batchIterator,
            indexNameResolver,
            collectExpressions,
            rowShardResolver,
            bulkShardProcessor,
            updateItemSupplier
        );
    }
}
