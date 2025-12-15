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

package io.crate.planner.operators;

import static io.crate.data.SentinelRow.SENTINEL;
import static io.crate.execution.engine.indexing.ShardingUpsertExecutor.BULK_REQUEST_TIMEOUT_SETTING;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_CLOSED_BLOCK;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SequencedCollection;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.StreamSupport;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreatePartitionsRequest;
import org.elasticsearch.action.admin.indices.create.TransportCreatePartitions;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.support.RetryableAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.OperationRouting;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.index.IndexNotFoundException;
import org.jetbrains.annotations.Nullable;

import com.carrotsearch.hppc.IntArrayList;

import io.crate.analyze.OrderBy;
import io.crate.analyze.SymbolEvaluator;
import io.crate.analyze.relations.TableFunctionRelation;
import io.crate.common.concurrent.ConcurrencyLimit;
import io.crate.data.CollectionBucket;
import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowConsumer;
import io.crate.data.RowN;
import io.crate.data.breaker.RamAccounting;
import io.crate.exceptions.ColumnValidationException;
import io.crate.exceptions.SQLExceptions;
import io.crate.execution.dml.BulkResponse;
import io.crate.execution.dml.IndexItem;
import io.crate.execution.dml.Indexer;
import io.crate.execution.dml.ShardRequest;
import io.crate.execution.dml.ShardResponse;
import io.crate.execution.dml.upsert.ShardUpsertAction;
import io.crate.execution.dml.upsert.ShardUpsertRequest;
import io.crate.execution.dsl.projection.ColumnIndexWriterProjection;
import io.crate.execution.dsl.projection.builder.InputColumns;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.execution.engine.collect.RowShardResolver;
import io.crate.execution.engine.indexing.GroupRowsByShard;
import io.crate.execution.engine.indexing.ItemFactory;
import io.crate.execution.engine.indexing.ShardLocation;
import io.crate.execution.engine.indexing.ShardedRequests;
import io.crate.execution.engine.indexing.ShardedRequests.ItemAndRoutingAndSourceInfo;
import io.crate.execution.engine.indexing.UpsertResultContext;
import io.crate.execution.jobs.NodeLimits;
import io.crate.expression.InputFactory;
import io.crate.expression.InputRow;
import io.crate.expression.symbol.Assignments;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.NodeContext;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import io.crate.metadata.ScopedRef;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.tablefunctions.TableFunctionImplementation;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.PlannerContext;
import io.crate.types.DataType;


public class InsertFromValues implements LogicalPlan {

    private final TableFunctionRelation tableFunctionRelation;
    private final ColumnIndexWriterProjection writerProjection;

    InsertFromValues(TableFunctionRelation tableFunctionRelation,
                     ColumnIndexWriterProjection writerProjection) {
        this.tableFunctionRelation = tableFunctionRelation;
        this.writerProjection = writerProjection;
    }

    @Override
    public StatementType type() {
        return StatementType.INSERT;
    }

    @Override
    public void executeOrFail(DependencyCarrier dependencies,
                              PlannerContext plannerContext,
                              RowConsumer consumer,
                              Row params,
                              SubQueryResults subQueryResults) {
        DocTableInfo tableInfo = dependencies
            .schemas()
            .getTableInfo(writerProjection.tableIdent());

        // For instance, the target table of the insert from values
        // statement is the table with the following schema:
        //
        // CREATE TABLE users (
        //      dep_id TEXT,
        //      name TEXT,
        //      id INT,
        //      country_id INT,
        //      PRIMARY KEY (dep_id, id, country_id))
        // CLUSTERED BY (dep_id)
        // PARTITIONED BY (country_id)
        //
        // The insert from values statement below would have the column
        // index writer projection of its plan that contains the column
        // idents and symbols required to create corresponding inputs.
        // The diagram below shows the projection's column symbols used
        // in the plan and relation between symbols sub-/sets.
        //
        //                        +------------------------+
        //                        |          +-------------+  PK symbols
        //    cluster by +------+ |          |      +------+
        //    symbol            | |          |      |
        //                      + +          +      +
        // INSERT INTO users (dep_id, name, id, country_id) VALUES (?, ?, ?, ?)
        //                       +      +    +     +   +
        //               +-------+      |    |     |   |
        //   all target  +--------------+    |     |   +---+  partitioned by
        //   column      +-------------------+     |          symbols
        //   symbols     +-------------------------+

        InputFactory inputFactory = new InputFactory(dependencies.nodeContext());
        InputFactory.Context<CollectExpression<Row, ?>> context =
            inputFactory.ctxForInputColumns(plannerContext.transactionContext());

        var allColumnSymbols = InputColumns.create(
            writerProjection.allTargetColumns(),
            new InputColumns.SourceSymbols(writerProjection.allTargetColumns()));

        ArrayList<Input<?>> insertInputs = new ArrayList<>(allColumnSymbols.size());
        for (Symbol symbol : allColumnSymbols) {
            insertInputs.add(context.add(symbol));
        }

        ArrayList<Input<?>> partitionedByInputs = new ArrayList<>(writerProjection.partitionedBySymbols().size());
        for (Symbol partitionedBySymbol : writerProjection.partitionedBySymbols()) {
            partitionedByInputs.add(context.add(partitionedBySymbol));
        }

        if (writerProjection.clusteredBy() != null) {
            context.add(writerProjection.clusteredBy());
        }

        String[] onConflictColumns;
        Symbol[] onConflictAssignments;
        if (writerProjection.onDuplicateKeyAssignments() == null) {
            onConflictColumns = null;
            onConflictAssignments = null;
        } else {
            Assignments assignments = Assignments.convert(
                writerProjection.onDuplicateKeyAssignments(),
                dependencies.nodeContext()
            );
            onConflictAssignments = assignments.bindSources(tableInfo, params, subQueryResults);
            onConflictColumns = assignments.targetNames();
        }

        var partitionResolver = PartitionName.createResolver(
            writerProjection.tableIdent(),
            writerProjection.partitionIdent(),
            partitionedByInputs
        );

        Map<PartitionName, Consumer<IndexItem>> validatorsCache = new HashMap<>();

        BiConsumer<PartitionName, IndexItem> constraintsChecker = (partition, indexItem) -> checkConstraints(
            indexItem,
            partition,
            tableInfo,
            plannerContext.transactionContext(),
            plannerContext.nodeContext(),
            validatorsCache,
            writerProjection.allTargetColumns()
        );

        GroupRowsByShard<ShardUpsertRequest, ShardUpsertRequest.Item> grouper =
            createRowsByShardGrouper(
                constraintsChecker,
                onConflictAssignments,
                insertInputs,
                partitionResolver,
                context,
                plannerContext,
                dependencies.clusterService());

        ArrayList<Row> rows = new ArrayList<>();
        evaluateValueTableFunction(
            tableFunctionRelation.functionImplementation(),
            tableFunctionRelation.function().arguments(),
            writerProjection.allTargetColumns(),
            tableInfo,
            params,
            plannerContext,
            subQueryResults
        ).forEachRemaining(rows::add);

        List<Symbol> returnValues = this.writerProjection.returnValues();

        ShardUpsertRequest.Builder builder = new ShardUpsertRequest.Builder(
            plannerContext.transactionContext().sessionSettings(),
            BULK_REQUEST_TIMEOUT_SETTING.get(dependencies.settings()),
            writerProjection.isIgnoreDuplicateKeys()
                ? ShardUpsertRequest.DuplicateKeyAction.IGNORE
                : ShardUpsertRequest.DuplicateKeyAction.UPDATE_OR_FAIL,
            rows.size() > 1, // continueOnErrors
            onConflictColumns,
            writerProjection.allTargetColumns().toArray(new ScopedRef[0]),
            returnValues.isEmpty() ? null : returnValues.toArray(new Symbol[0]),
            plannerContext.jobId()
        );

        var shardedRequests = new ShardedRequests<>(builder::newRequest, RamAccounting.NO_ACCOUNTING);

        for (Row row : rows) {
            try {
                grouper.apply(shardedRequests, row, true);
            } catch (Throwable t) {
                consumer.accept(null, t);
                return;
            }
        }
        validatorsCache.clear();

        createPartitions(
            tableInfo,
            dependencies.client(),
            shardedRequests.itemsByMissingPartition().keySet(),
            dependencies.clusterService()
        ).thenCompose(_ -> {
            var shardUpsertRequests = resolveAndGroupShardRequests(
                shardedRequests,
                dependencies.clusterService()).values();
            return execute(
                dependencies.nodeLimits(),
                dependencies.clusterService().state(),
                shardUpsertRequests,
                dependencies.client(),
                dependencies.scheduler(),
                tableInfo.isPartitioned());
        }).whenComplete((response, t) -> {
            if (t == null) {
                if (returnValues.isEmpty()) {
                    consumer.accept(InMemoryBatchIterator.of(new Row1((long) response.numSuccessfulWrites()), SENTINEL),
                                    null);
                } else {
                    consumer.accept(InMemoryBatchIterator.of(new CollectionBucket(response.resultRows()), SENTINEL, false), null);
                }
            } else {
                consumer.accept(null, t);
            }
        });
    }

    @Override
    public CompletableFuture<BulkResponse> executeBulk(DependencyCarrier dependencies,
                                                       PlannerContext plannerContext,
                                                       List<Row> bulkParams,
                                                       SubQueryResults subQueryResults) {
        final DocTableInfo tableInfo = dependencies
            .schemas()
            .getTableInfo(writerProjection.tableIdent());

        String[] updateColumnNames;
        Assignments assignments;
        if (writerProjection.onDuplicateKeyAssignments() == null) {
            assignments = null;
            updateColumnNames = null;
        } else {
            assignments = Assignments.convert(writerProjection.onDuplicateKeyAssignments(), dependencies.nodeContext());
            updateColumnNames = assignments.targetNames();
        }

        InputFactory inputFactory = new InputFactory(dependencies.nodeContext());
        InputFactory.Context<CollectExpression<Row, ?>> context =
            inputFactory.ctxForInputColumns(plannerContext.transactionContext());

        var allColumnSymbols = InputColumns.create(
            writerProjection.allTargetColumns(),
            new InputColumns.SourceSymbols(writerProjection.allTargetColumns()));

        ArrayList<Input<?>> insertInputs = new ArrayList<>(allColumnSymbols.size());
        for (Symbol symbol : allColumnSymbols) {
            insertInputs.add(context.add(symbol));
        }

        ArrayList<Input<?>> partitionedByInputs = new ArrayList<>(writerProjection.partitionedBySymbols().size());
        for (Symbol partitionedBySymbol : writerProjection.partitionedBySymbols()) {
            partitionedByInputs.add(context.add(partitionedBySymbol));
        }

        if (writerProjection.clusteredBy() != null) {
            context.add(writerProjection.clusteredBy());
        }

        var partitionResolver = PartitionName.createResolver(
            writerProjection.tableIdent(),
            writerProjection.partitionIdent(),
            partitionedByInputs
        );

        ShardUpsertRequest.Builder builder = new ShardUpsertRequest.Builder(
            plannerContext.transactionContext().sessionSettings(),
            BULK_REQUEST_TIMEOUT_SETTING.get(dependencies.settings()),
            writerProjection.isIgnoreDuplicateKeys()
                ? ShardUpsertRequest.DuplicateKeyAction.IGNORE
                : ShardUpsertRequest.DuplicateKeyAction.UPDATE_OR_FAIL,
            true, // continueOnErrors
            updateColumnNames,
            writerProjection.allTargetColumns().toArray(new ScopedRef[0]),
            null,
            plannerContext.jobId()
        );
        var shardedRequests = new ShardedRequests<>(builder::newRequest, RamAccounting.NO_ACCOUNTING);

        HashMap<PartitionName, Consumer<IndexItem>> validatorsCache = new HashMap<>();
        BiConsumer<PartitionName, IndexItem> constraintsChecker = (partition, indexItem) -> checkConstraints(
            indexItem,
            partition,
            tableInfo,
            plannerContext.transactionContext(),
            plannerContext.nodeContext(),
            validatorsCache,
            writerProjection.allTargetColumns()
        );


        var bulkResponse = new BulkResponse(bulkParams.size());
        IntArrayList bulkIndices = new IntArrayList();
        CompletableFuture<BulkResponse> result = new CompletableFuture<>();
        for (int bulkIdx = 0; bulkIdx < bulkParams.size(); bulkIdx++) {
            Row param = bulkParams.get(bulkIdx);
            final Symbol[] assignmentSources;
            if (assignments != null) {
                assignmentSources = assignments.bindSources(tableInfo, param, subQueryResults);
            } else {
                assignmentSources = null;
            }

            GroupRowsByShard<ShardUpsertRequest, ShardUpsertRequest.Item> grouper =
                createRowsByShardGrouper(
                    constraintsChecker,
                    assignmentSources,
                    insertInputs,
                    partitionResolver,
                    context,
                    plannerContext,
                    dependencies.clusterService());

            try {
                Iterator<Row> rows = evaluateValueTableFunction(
                    tableFunctionRelation.functionImplementation(),
                    tableFunctionRelation.function().arguments(),
                    writerProjection.allTargetColumns(),
                    tableInfo,
                    param,
                    plannerContext,
                    subQueryResults);

                while (rows.hasNext()) {
                    Row row = rows.next();
                    grouper.apply(shardedRequests, row, true);
                    bulkIndices.add(bulkIdx);
                }
            } catch (Throwable t) {
                result.completeExceptionally(t);
                return result;
            }
        }
        validatorsCache.clear();

        createPartitions(
            tableInfo,
            dependencies.client(),
            shardedRequests.itemsByMissingPartition().keySet(),
            dependencies.clusterService()
        ).thenCompose(_ -> {
            var shardUpsertRequests = resolveAndGroupShardRequests(
                shardedRequests,
                dependencies.clusterService()).values();
            return execute(
                dependencies.nodeLimits(),
                dependencies.clusterService().state(),
                shardUpsertRequests,
                dependencies.client(),
                dependencies.scheduler(),
                tableInfo.isPartitioned());
        }).whenComplete((response, t) -> {
            if (t == null) {
                bulkResponse.update(response, bulkIndices);
                result.complete(bulkResponse);
            } else {
                result.completeExceptionally(t);
            }
        });
        return result;
    }

    private GroupRowsByShard<ShardUpsertRequest, ShardUpsertRequest.Item>
        createRowsByShardGrouper(BiConsumer<PartitionName, IndexItem> constraintsChecker,
                                 Symbol[] onConflictAssignments,
                                 ArrayList<Input<?>> insertInputs,
                                 Supplier<PartitionName> partitionResolver,
                                 InputFactory.Context<CollectExpression<Row, ?>> collectContext,
                                 PlannerContext plannerContext,
                                 ClusterService clusterService) {
        InputRow insertValues = new InputRow(insertInputs);
        ItemFactory<ShardUpsertRequest.Item> itemFactory = (id, pkValues, autoGeneratedTimestamp) ->
            ShardUpsertRequest.Item.forInsert(
                id,
                pkValues,
                autoGeneratedTimestamp,
                writerProjection.allTargetColumns().toArray(ScopedRef[]::new),
                insertValues.materialize(),
                onConflictAssignments,
                0
            );

        var rowShardResolver = new RowShardResolver(
            plannerContext.transactionContext(),
            plannerContext.nodeContext(),
            writerProjection.primaryKeys(),
            writerProjection.ids(),
            writerProjection.clusteredByIdent(),
            writerProjection.clusteredBy());

        return new GroupRowsByShard<>(
            clusterService,
            constraintsChecker,
            rowShardResolver,
            partitionResolver,
            collectContext.expressions(),
            itemFactory,
            true,
            UpsertResultContext.forRowCount()
        );
    }

    public static void checkConstraints(@Nullable IndexItem indexItem,
                                        PartitionName partitionName,
                                        DocTableInfo tableInfo,
                                        TransactionContext txnCtx,
                                        NodeContext nodeCtx,
                                        Map<PartitionName, Consumer<IndexItem>> validatorsCache,
                                        List<ScopedRef> targetColumns) {
        if (indexItem == null) {
            return;
        }
        var validator = validatorsCache.computeIfAbsent(
            partitionName,
            _ -> Indexer.createConstraintCheck(
                tableInfo,
                partitionName.values(),
                txnCtx,
                nodeCtx,
                targetColumns
            )
        );
        validator.accept(indexItem);
    }

    @SuppressWarnings("unchecked")
    private static Iterator<Row> evaluateValueTableFunction(TableFunctionImplementation<?> funcImplementation,
                                                            List<Symbol> arguments,
                                                            List<ScopedRef> allTargetReferences,
                                                            DocTableInfo tableInfo,
                                                            Row params,
                                                            PlannerContext plannerContext,
                                                            SubQueryResults subQueryResults) {
        SymbolEvaluator symbolEval = new SymbolEvaluator(
            plannerContext.transactionContext(),
            plannerContext.nodeContext(),
            subQueryResults);
        Function<? super Symbol, Input<?>> eval = (symbol) -> symbol.accept(symbolEval, params);

        ArrayList<Input<?>> boundArguments = new ArrayList<>(arguments.size());
        for (int i = 0; i < arguments.size(); i++) {
            boundArguments.add(eval.apply(arguments.get(i)));
        }
        Iterable<Row> rows = funcImplementation.evaluate(
            plannerContext.transactionContext(),
            plannerContext.nodeContext(),
            boundArguments.toArray(new Input[0]));

        return StreamSupport.stream(rows.spliterator(), false)
            .map(row -> cast(row, allTargetReferences, tableInfo))
            .iterator();
    }

    private static Row cast(Row row, List<ScopedRef> columnReferences, DocTableInfo tableInfo) {
        if (row == null) {
            return null;
        }
        Object[] cells = new Object[row.numColumns()];
        for (int i = 0; i < cells.length; i++) {
            var reference = columnReferences.get(i);
            DataType<?> targetType = reference.valueType();
            Object value = row.get(i);
            try {
                cells[i] = targetType.implicitCast(value);
            } catch (IllegalArgumentException | ClassCastException e) {
                throw new ColumnValidationException(
                    reference.column().name(),
                    tableInfo.ident(),
                    "Invalid value '" + value + "' for type '" + targetType + "'");
            }
        }
        return new RowN(cells);
    }

    private static ShardLocation getShardLocation(PartitionName partitionName,
                                                  String id,
                                                  @Nullable String routing,
                                                  OperationRouting operationRouting,
                                                  ClusterState state) {
        Metadata metadata = state.metadata();
        String indexUUID = metadata.getIndex(partitionName.relationName(), partitionName.values(), true, IndexMetadata::getIndexUUID);
        if (indexUUID == null) {
            throw new IndexNotFoundException(partitionName.asIndexName());
        }
        ShardIterator shardIterator = operationRouting.indexShards(
            state,
            indexUUID,
            id,
            routing);

        final String nodeId;
        ShardRouting shardRouting = shardIterator.nextOrNull();
        if (shardRouting == null) {
            nodeId = null;
        } else if (shardRouting.active() == false) {
            nodeId = shardRouting.relocatingNodeId();
        } else {
            nodeId = shardRouting.currentNodeId();
        }
        return new ShardLocation(shardIterator.shardId(), nodeId);
    }

    private static <TReq extends ShardRequest<TReq, TItem>, TItem extends ShardRequest.Item>
        Map<ShardLocation, TReq> resolveAndGroupShardRequests(ShardedRequests<TReq, TItem> shardedRequests,
                                                          ClusterService clusterService) {
        var itemsByMissingPartition = shardedRequests.itemsByMissingPartition().entrySet().iterator();
        ClusterState state = clusterService.state();
        OperationRouting operationRouting = clusterService.operationRouting();
        while (itemsByMissingPartition.hasNext()) {
            var entry = itemsByMissingPartition.next();
            PartitionName partition = entry.getKey();
            List<ItemAndRoutingAndSourceInfo<TItem>> requestItems = entry.getValue();

            var requestItemsIterator = requestItems.iterator();
            while (requestItemsIterator.hasNext()) {
                var itemAndRoutingAndSourceInfo = requestItemsIterator.next();
                ShardLocation shardLocation;
                try {
                    shardLocation = getShardLocation(
                        partition,
                        itemAndRoutingAndSourceInfo.item().id(),
                        itemAndRoutingAndSourceInfo.routing(),
                        operationRouting,
                        state
                    );
                } catch (IndexNotFoundException e) {
                    requestItemsIterator.remove();
                    continue;
                }
                shardedRequests.add(itemAndRoutingAndSourceInfo.item(), shardLocation, null);
                requestItemsIterator.remove();
            }
            if (requestItems.isEmpty()) {
                itemsByMissingPartition.remove();
            }
        }

        return shardedRequests.itemsByShard();
    }

    private CompletableFuture<ShardResponse.CompressedResult> execute(NodeLimits nodeLimits,
                                                                      ClusterState state,
                                                                      Collection<ShardUpsertRequest> shardUpsertRequests,
                                                                      Client client,
                                                                      ScheduledExecutorService scheduler,
                                                                      boolean isPartitioned) {
        ShardResponse.CompressedResult compressedResult = new ShardResponse.CompressedResult();
        if (shardUpsertRequests.isEmpty()) {
            return CompletableFuture.completedFuture(compressedResult);
        }

        CompletableFuture<ShardResponse.CompressedResult> result = new CompletableFuture<>();
        AtomicInteger numRequests = new AtomicInteger(shardUpsertRequests.size());
        AtomicReference<Throwable> lastFailure = new AtomicReference<>(null);

        Consumer<ShardUpsertRequest> countdown = _ -> {
            if (numRequests.decrementAndGet() == 0) {
                Throwable throwable = lastFailure.get();
                if (throwable == null) {
                    result.complete(compressedResult);
                } else {
                    throwable = SQLExceptions.unwrap(throwable);
                    // we want to report duplicate key exceptions
                    if (!SQLExceptions.isDocumentAlreadyExistsException(throwable) &&
                            (partitionWasDeleted(throwable, isPartitioned)
                                    || partitionClosed(throwable, isPartitioned)
                                    || mixedArgumentTypesFailure(throwable))) {
                        result.complete(compressedResult);
                    } else {
                        result.completeExceptionally(throwable);
                    }
                }
            }
        };
        for (ShardUpsertRequest request : shardUpsertRequests) {
            String nodeId;
            try {
                nodeId = state.routingTable()
                    .shardRoutingTable(request.shardId())
                    .primaryShard()
                    .currentNodeId();
            } catch (IndexNotFoundException e) {
                lastFailure.set(e);
                if (!isPartitioned) {
                    synchronized (compressedResult) {
                        compressedResult.markAsFailed(request.items());
                    }
                }
                countdown.accept(request);
                continue;
            }
            final ConcurrencyLimit nodeLimit = nodeLimits.get(nodeId);
            final long startTime = nodeLimit.startSample();

            ActionListener<ShardResponse> listener = new ActionListener<>() {
                @Override
                public void onResponse(ShardResponse shardResponse) {
                    Throwable throwable = shardResponse.failure();
                    if (throwable == null) {
                        nodeLimit.onSample(startTime);
                        synchronized (compressedResult) {
                            compressedResult.update(shardResponse);
                        }
                    } else {
                        nodeLimit.onSample(startTime);
                        lastFailure.set(throwable);
                    }
                    countdown.accept(request);
                }

                @Override
                public void onFailure(Exception e) {
                    nodeLimit.onSample(startTime);
                    Throwable t = SQLExceptions.unwrap(e);
                    if (!partitionWasDeleted(t, isPartitioned)) {
                        synchronized (compressedResult) {
                            compressedResult.markAsFailed(request.items());
                        }
                    }
                    lastFailure.set(t);
                    countdown.accept(request);
                }
            };

            RetryableAction<ShardResponse> retryableAction = RetryableAction.of(
                scheduler,
                l -> client.execute(ShardUpsertAction.INSTANCE, request).whenComplete(l),
                BackoffPolicy.limitedDynamic(nodeLimit),
                listener
            );
            retryableAction.run();
        }
        return result;
    }

    private static boolean mixedArgumentTypesFailure(Throwable throwable) {
        return throwable instanceof ClassCastException;
    }

    private static boolean partitionWasDeleted(Throwable throwable, boolean isPartitioned) {
        return throwable instanceof IndexNotFoundException && isPartitioned;
    }

    private static boolean partitionClosed(Throwable throwable, boolean isPartitioned) {
        if (throwable instanceof ClusterBlockException blockException && isPartitioned) {
            for (ClusterBlock clusterBlock : blockException.blocks()) {
                if (clusterBlock.id() == INDEX_CLOSED_BLOCK.id()) {
                    return true;
                }
            }
        }
        return false;
    }

    private static CompletableFuture<AcknowledgedResponse> createPartitions(DocTableInfo tableInfo,
                                                                            Client client,
                                                                            Set<PartitionName> partitions,
                                                                            ClusterService clusterService) {
        List<PartitionName> partitionsToCreate = new ArrayList<>();
        for (var partition : partitions) {
            String indexUUID = clusterService.state().metadata()
                .getIndex(tableInfo.ident(), partition.values(), true, IndexMetadata::getIndexUUID);
            if (indexUUID == null) {
                partitionsToCreate.add(partition);
            }
        }
        if (partitionsToCreate.isEmpty()) {
            return CompletableFuture.completedFuture(new AcknowledgedResponse(true));
        }
        return client.execute(TransportCreatePartitions.ACTION, CreatePartitionsRequest.of(partitionsToCreate));
    }

    @Override
    public ExecutionPlan build(DependencyCarrier executor,
                               PlannerContext plannerContext,
                               Set<PlanHint> hints,
                               ProjectionBuilder projectionBuilder,
                               int limit,
                               int offset,
                               @Nullable OrderBy order,
                               @Nullable Integer pageSizeHint,
                               Row params,
                               SubQueryResults subQueryResults) {
        return null;
    }

    @Override
    public List<Symbol> outputs() {
        return List.of();
    }

    @Override
    public List<RelationName> relationNames() {
        return List.of();
    }

    @Override
    public List<LogicalPlan> sources() {
        return List.of();
    }

    @Override
    public LogicalPlan replaceSources(List<LogicalPlan> sources) {
        return this;
    }

    @Override
    public LogicalPlan pruneOutputsExcept(SequencedCollection<Symbol> outputsToKeep) {
        return this;
    }

    @Override
    public Map<LogicalPlan, SelectSymbol> dependencies() {
        return Map.of();
    }

    @Override
    public <C, R> R accept(LogicalPlanVisitor<C, R> visitor, C context) {
        return visitor.visitInsert(this, context);
    }
}
