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
import java.util.Arrays;
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
import org.elasticsearch.action.admin.indices.create.CreatePartitionsAction;
import org.elasticsearch.action.admin.indices.create.CreatePartitionsRequest;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.Metadata;
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
import io.crate.execution.dml.IndexItem;
import io.crate.execution.dml.Indexer;
import io.crate.execution.dml.ShardRequest;
import io.crate.execution.dml.ShardResponse;
import io.crate.execution.dml.upsert.ShardUpsertAction;
import io.crate.execution.dml.upsert.ShardUpsertRequest;
import io.crate.execution.dml.upsert.ShardUpsertRequest.Item;
import io.crate.execution.dsl.projection.ColumnIndexWriterProjection;
import io.crate.execution.dsl.projection.builder.InputColumns;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.execution.engine.collect.RowShardResolver;
import io.crate.execution.engine.indexing.GroupRowsByShard;
import io.crate.execution.engine.indexing.IndexNameResolver;
import io.crate.execution.engine.indexing.ItemFactory;
import io.crate.execution.engine.indexing.ShardLocation;
import io.crate.execution.engine.indexing.ShardedRequests;
import io.crate.execution.jobs.NodeLimits;
import io.crate.execution.support.RetryListener;
import io.crate.expression.InputFactory;
import io.crate.expression.InputRow;
import io.crate.expression.symbol.Assignments;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.IndexParts;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
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
    public void execute(DependencyCarrier dependencies,
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

        ArrayList<Input<?>> primaryKeyInputs = new ArrayList<>(writerProjection.ids().size());
        for (Symbol symbol : writerProjection.ids()) {
            primaryKeyInputs.add(context.add(symbol));
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
        var indexNameResolver = IndexNameResolver.create(
            writerProjection.tableIdent(),
            writerProjection.partitionIdent(),
            partitionedByInputs);

        Map<String, Consumer<IndexItem>> validatorsCache = new HashMap<>();

        BiConsumer<String, IndexItem> constraintsChecker = (indexName, indexItem) -> checkConstraints(
            indexItem,
            indexName,
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
                indexNameResolver,
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
            writerProjection.allTargetColumns().toArray(new Reference[0]),
            returnValues.isEmpty() ? null : returnValues.toArray(new Symbol[0]),
            plannerContext.jobId()
        );

        var shardedRequests = new ShardedRequests<>(builder::newRequest, RamAccounting.NO_ACCOUNTING);

        for (Row row : rows) {
            try {
                Item item = grouper.apply(shardedRequests, row, true);
                // Primary Key and CLUSTERED BY check is already done in grouper -> RowShardResolver, both cannot be null.
                // constraintsChecker is also used in grouper.apply but only for partitioned tables.
                // We keep the check below so that insert from values into regular tables doesn't send invalid values to the shards and fails early.
                constraintsChecker.accept(indexNameResolver.get(), item);
            } catch (Throwable t) {
                consumer.accept(null, t);
                return;
            }
        }
        validatorsCache.clear();

        createIndices(
            dependencies.client(),
            shardedRequests.itemsByMissingIndex().keySet(),
            dependencies.clusterService()
        ).thenCompose(acknowledgedResponse -> {
            var shardUpsertRequests = resolveAndGroupShardRequests(
                shardedRequests,
                dependencies.clusterService()).values();
            return execute(
                dependencies.nodeLimits(),
                dependencies.clusterService().state(),
                shardUpsertRequests,
                dependencies.client(),
                dependencies.scheduler());
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
    public List<CompletableFuture<Long>> executeBulk(DependencyCarrier dependencies,
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

        ArrayList<Input<?>> primaryKeyInputs = new ArrayList<>(writerProjection.ids().size());
        for (Symbol symbol : writerProjection.ids()) {
            primaryKeyInputs.add(context.add(symbol));
        }
        if (writerProjection.clusteredBy() != null) {
            context.add(writerProjection.clusteredBy());
        }

        var indexNameResolver = IndexNameResolver.create(
            writerProjection.tableIdent(),
            writerProjection.partitionIdent(),
            partitionedByInputs);

        ShardUpsertRequest.Builder builder = new ShardUpsertRequest.Builder(
            plannerContext.transactionContext().sessionSettings(),
            BULK_REQUEST_TIMEOUT_SETTING.get(dependencies.settings()),
            writerProjection.isIgnoreDuplicateKeys()
                ? ShardUpsertRequest.DuplicateKeyAction.IGNORE
                : ShardUpsertRequest.DuplicateKeyAction.UPDATE_OR_FAIL,
            true, // continueOnErrors
            updateColumnNames,
            writerProjection.allTargetColumns().toArray(new Reference[0]),
            null,
            plannerContext.jobId()
        );
        var shardedRequests = new ShardedRequests<>(builder::newRequest, RamAccounting.NO_ACCOUNTING);

        HashMap<String, Consumer<IndexItem>> validatorsCache = new HashMap<>();

        BiConsumer<String, IndexItem> constraintsChecker = (indexName, indexItem) -> checkConstraints(
            indexItem,
            indexName,
            tableInfo,
            plannerContext.transactionContext(),
            plannerContext.nodeContext(),
            validatorsCache,
            writerProjection.allTargetColumns()
        );


        IntArrayList bulkIndices = new IntArrayList();
        List<CompletableFuture<Long>> results = createUnsetFutures(bulkParams.size());
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
                    indexNameResolver,
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
                    Item item = grouper.apply(shardedRequests, row, true);

                    // Primary Key and CLUSTERED BY check is already done in grouper -> RowShardResolver, both cannot be null.
                    // constraintsChecker is also used in grouper.apply but only for partitioned tables.
                    // We keep the check below so that insert from values into regular tables doesn't send invalid values to the shards and fails early.
                    constraintsChecker.accept(indexNameResolver.get(), item);
                    bulkIndices.add(bulkIdx);
                }
            } catch (Throwable t) {
                for (CompletableFuture<Long> result : results) {
                    result.completeExceptionally(t);
                }
                return results;
            }
        }
        validatorsCache.clear();

        createIndices(
            dependencies.client(),
            shardedRequests.itemsByMissingIndex().keySet(),
            dependencies.clusterService()
        ).thenCompose(acknowledgedResponse -> {
            var shardUpsertRequests = resolveAndGroupShardRequests(
                shardedRequests,
                dependencies.clusterService()).values();
            return execute(
                dependencies.nodeLimits(),
                dependencies.clusterService().state(),
                shardUpsertRequests,
                dependencies.client(),
                dependencies.scheduler());
        }).whenComplete((response, t) -> {
            if (t == null) {
                long[] resultRowCount = createBulkResponse(response, bulkParams.size(), bulkIndices);
                for (int i = 0; i < bulkParams.size(); i++) {
                    results.get(i).complete(resultRowCount[i]);
                }
            } else {
                for (CompletableFuture<Long> result : results) {
                    result.completeExceptionally(t);
                }
            }
        });
        return results;
    }

    private GroupRowsByShard<ShardUpsertRequest, ShardUpsertRequest.Item>
        createRowsByShardGrouper(BiConsumer<String, IndexItem> constraintsChecker,
                                 Symbol[] onConflictAssignments,
                                 ArrayList<Input<?>> insertInputs,
                                 Supplier<String> indexNameResolver,
                                 InputFactory.Context<CollectExpression<Row, ?>> collectContext,
                                 PlannerContext plannerContext,
                                 ClusterService clusterService) {
        InputRow insertValues = new InputRow(insertInputs);
        ItemFactory<ShardUpsertRequest.Item> itemFactory = (id, pkValues, autoGeneratedTimestamp) ->
            ShardUpsertRequest.Item.forInsert(
                id,
                pkValues,
                autoGeneratedTimestamp,
                insertValues.materialize(),
                onConflictAssignments
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
            indexNameResolver,
            collectContext.expressions(),
            itemFactory,
            true
        );
    }

    public static void checkConstraints(@Nullable IndexItem indexItem,
                                        String indexName,
                                        DocTableInfo tableInfo,
                                        TransactionContext txnCtx,
                                        NodeContext nodeCtx,
                                        Map<String, Consumer<IndexItem>> validatorsCache,
                                        List<Reference> targetColumns) {
        if (indexItem == null) {
            return;
        }
        var validator = validatorsCache.computeIfAbsent(
            indexName,
            index -> Indexer.createConstraintCheck(
                indexName,
                tableInfo,
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
                                                            List<Reference> allTargetReferences,
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

    private static Row cast(Row row, List<Reference> columnReferences, DocTableInfo tableInfo) {
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

    private static ShardLocation getShardLocation(String indexName,
                                                  String id,
                                                  @Nullable String routing,
                                                  ClusterService clusterService) {
        ShardIterator shardIterator = clusterService.operationRouting().indexShards(
            clusterService.state(),
            indexName,
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
        var itemsByMissingIndex = shardedRequests.itemsByMissingIndex().entrySet().iterator();
        while (itemsByMissingIndex.hasNext()) {
            var entry = itemsByMissingIndex.next();
            var index = entry.getKey();
            var requestItems = entry.getValue();

            var requestItemsIterator = requestItems.iterator();
            while (requestItemsIterator.hasNext()) {
                var itemAndRoutingAndSourceInfo = requestItemsIterator.next();
                ShardLocation shardLocation;
                try {
                    shardLocation = getShardLocation(
                        index,
                        itemAndRoutingAndSourceInfo.item().id(),
                        itemAndRoutingAndSourceInfo.routing(),
                        clusterService);
                } catch (IndexNotFoundException e) {
                    if (IndexParts.isPartitioned(index)) {
                        requestItemsIterator.remove();
                        continue;
                    } else {
                        throw e;
                    }
                }
                shardedRequests.add(itemAndRoutingAndSourceInfo.item(), shardLocation, null);
                requestItemsIterator.remove();
            }
            if (requestItems.isEmpty()) {
                itemsByMissingIndex.remove();
            }
        }

        return shardedRequests.itemsByShard();
    }

    private CompletableFuture<ShardResponse.CompressedResult> execute(NodeLimits nodeLimits,
                                                                      ClusterState state,
                                                                      Collection<ShardUpsertRequest> shardUpsertRequests,
                                                                      ElasticsearchClient elasticsearchClient,
                                                                      ScheduledExecutorService scheduler) {
        ShardResponse.CompressedResult compressedResult = new ShardResponse.CompressedResult();
        if (shardUpsertRequests.isEmpty()) {
            return CompletableFuture.completedFuture(compressedResult);
        }

        CompletableFuture<ShardResponse.CompressedResult> result = new CompletableFuture<>();
        AtomicInteger numRequests = new AtomicInteger(shardUpsertRequests.size());
        AtomicReference<Throwable> lastFailure = new AtomicReference<>(null);

        Consumer<ShardUpsertRequest> countdown = request -> {
            if (numRequests.decrementAndGet() == 0) {
                Throwable throwable = lastFailure.get();
                if (throwable == null) {
                    result.complete(compressedResult);
                } else {
                    throwable = SQLExceptions.unwrap(throwable);
                    // we want to report duplicate key exceptions
                    if (!SQLExceptions.isDocumentAlreadyExistsException(throwable) &&
                            (partitionWasDeleted(throwable, request.index())
                                    || partitionClosed(throwable, request.index())
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
                if (!IndexParts.isPartitioned(request.index())) {
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
                        nodeLimit.onSample(startTime, false);
                        synchronized (compressedResult) {
                            compressedResult.update(shardResponse);
                        }
                    } else {
                        nodeLimit.onSample(startTime, true);
                        lastFailure.set(throwable);
                    }
                    countdown.accept(request);
                }

                @Override
                public void onFailure(Exception e) {
                    nodeLimit.onSample(startTime, true);
                    Throwable t = SQLExceptions.unwrap(e);
                    if (!partitionWasDeleted(t, request.index())) {
                        synchronized (compressedResult) {
                            compressedResult.markAsFailed(request.items());
                        }
                    }
                    lastFailure.set(t);
                    countdown.accept(request);
                }
            };

            elasticsearchClient.execute(ShardUpsertAction.INSTANCE, request)
                .whenComplete(new RetryListener<>(
                    scheduler,
                    l -> elasticsearchClient.execute(ShardUpsertAction.INSTANCE, request)
                        .whenComplete(l),
                    listener,
                    BackoffPolicy.limitedDynamic(nodeLimit)
                ));
        }
        return result;
    }

    private static boolean mixedArgumentTypesFailure(Throwable throwable) {
        return throwable instanceof ClassCastException;
    }

    private static boolean partitionWasDeleted(Throwable throwable, String index) {
        return throwable instanceof IndexNotFoundException && IndexParts.isPartitioned(index);
    }

    private static boolean partitionClosed(Throwable throwable, String index) {
        if (throwable instanceof ClusterBlockException blockException && IndexParts.isPartitioned(index)) {
            for (ClusterBlock clusterBlock : blockException.blocks()) {
                if (clusterBlock.id() == INDEX_CLOSED_BLOCK.id()) {
                    return true;
                }
            }
        }
        return false;
    }

    private static CompletableFuture<AcknowledgedResponse> createIndices(ElasticsearchClient elasticsearchClient,
                                                                         Set<String> indices,
                                                                         ClusterService clusterService) {
        Metadata metadata = clusterService.state().metadata();
        List<String> indicesToCreate = new ArrayList<>();
        for (var index : indices) {
            if (IndexParts.isPartitioned(index) && metadata.hasIndex(index) == false) {
                indicesToCreate.add(index);
            }
        }
        if (indicesToCreate.isEmpty()) {
            return CompletableFuture.completedFuture(new AcknowledgedResponse(true));
        }
        return elasticsearchClient.execute(CreatePartitionsAction.INSTANCE, new CreatePartitionsRequest(indicesToCreate));
    }

    /**
     * Create bulk-response depending on number of bulk responses
     * <pre>
     *     compressedResult
     *          success: [1, 1, 1, 1]
     *          failure: []
     *
     *     insert into t (x) values (?), (?)   -- bulkParams: [[1, 2], [3, 4]]
     *     Response:
     *      [2, 2]
     *
     *     insert into t (x) values (?)        -- bulkParams: [[1], [2], [3], [4]]
     *     Response:
     *      [1, 1, 1, 1]
     * </pre>
     */
    private static long[] createBulkResponse(ShardResponse.CompressedResult result,
                                             int bulkResponseSize,
                                             IntArrayList bulkIndices) {
        long[] resultRowCount = new long[bulkResponseSize];
        Arrays.fill(resultRowCount, 0L);
        for (int i = 0; i < bulkIndices.size(); i++) {
            int resultIdx = bulkIndices.get(i);
            if (result.successfulWrites(i)) {
                resultRowCount[resultIdx]++;
            } else if (result.failed(i)) {
                resultRowCount[resultIdx] = Row1.ERROR;
            }
        }
        return resultRowCount;
    }

    private static <T> List<CompletableFuture<T>> createUnsetFutures(int num) {
        ArrayList<CompletableFuture<T>> results = new ArrayList<>(num);
        for (int i = 0; i < num; i++) {
            results.add(new CompletableFuture<>());
        }
        return results;
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
