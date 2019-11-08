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

package io.crate.planner.operators;

import com.carrotsearch.hppc.IntArrayList;
import com.google.common.collect.Iterables;
import io.crate.action.FutureActionListener;
import io.crate.action.LimitedExponentialBackoff;
import io.crate.analyze.OrderBy;
import io.crate.analyze.SymbolEvaluator;
import io.crate.analyze.relations.AbstractTableRelation;
import io.crate.analyze.relations.TableFunctionRelation;
import io.crate.breaker.TypeGuessEstimateRowSize;
import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowConsumer;
import io.crate.data.RowN;
import io.crate.exceptions.ColumnValidationException;
import io.crate.exceptions.SQLExceptions;
import io.crate.execution.dml.ShardRequest;
import io.crate.execution.dml.ShardResponse;
import io.crate.execution.dml.upsert.GeneratedColumns;
import io.crate.execution.dml.upsert.InsertSourceFromCells;
import io.crate.execution.dml.upsert.ShardUpsertRequest;
import io.crate.execution.dml.upsert.TransportShardUpsertAction;
import io.crate.execution.dsl.projection.ColumnIndexWriterProjection;
import io.crate.execution.dsl.projection.builder.InputColumns;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.execution.engine.collect.RowShardResolver;
import io.crate.execution.engine.indexing.GroupRowsByShard;
import io.crate.execution.engine.indexing.IndexNameResolver;
import io.crate.execution.engine.indexing.ShardLocation;
import io.crate.execution.engine.indexing.ShardedRequests;
import io.crate.execution.support.RetryListener;
import io.crate.expression.InputFactory;
import io.crate.expression.InputRow;
import io.crate.expression.symbol.Assignments;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.IndexParts;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.Operation;
import io.crate.metadata.tablefunctions.TableFunctionImplementation;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.PlannerContext;
import io.crate.types.DataType;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreatePartitionsRequest;
import org.elasticsearch.action.admin.indices.create.TransportCreatePartitionsAction;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NotSerializableExceptionWrapper;
import org.elasticsearch.index.IndexNotFoundException;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.StreamSupport;

import static io.crate.data.SentinelRow.SENTINEL;
import static io.crate.execution.engine.indexing.ShardingUpsertExecutor.BULK_REQUEST_TIMEOUT_SETTING;
import static java.util.stream.Collectors.toList;

public class InsertFromValues implements LogicalPlan {

    private static final BackoffPolicy BACK_OFF_POLICY = LimitedExponentialBackoff.limitedExponential(1000);

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
            .getTableInfo(
                RelationName.fromIndexName(writerProjection.tableIdent().fqn()),
                Operation.INSERT);

        InputFactory inputFactory = new InputFactory(dependencies.functions());
        InputFactory.Context<CollectExpression<Row, ?>> context = inputFactory.ctxForInputColumns(plannerContext.transactionContext());

        var columnSymbols = InputColumns.create(
            writerProjection.allTargetReferences(),
            new InputColumns.SourceSymbols(writerProjection.allTargetReferences()));
        ArrayList<Input<?>> insertInputs = new ArrayList<>(columnSymbols.size());
        for (Symbol symbol : columnSymbols) {
            insertInputs.add(context.add(symbol));
        }
        ArrayList<Input<?>> partitionedByInputs = new ArrayList<>(writerProjection.partitionedBySymbols().size());
        for (Symbol partitionedBySymbol : writerProjection.partitionedBySymbols()) {
            partitionedByInputs.add(context.add(partitionedBySymbol));
        }

        ArrayList<Input<?>> primaryKeys = new ArrayList<>(writerProjection.columnSymbols().size());
        for (Symbol symbol : writerProjection.ids()) {
            primaryKeys.add(context.add(symbol));
        }
        Input<?> clusterByInput;
        if (writerProjection.clusteredBy() != null) {
            clusterByInput = context.add(writerProjection.clusteredBy());
        } else {
            clusterByInput = null;
        }

        var updateAssignments = writerProjection.onDuplicateKeyAssignments();
        String[] updateColumnNames;
        Symbol[] assignmentSources;
        if (updateAssignments == null) {
            updateColumnNames = null;
            assignmentSources = null;
        } else {
            Assignments assignments = Assignments.convert(updateAssignments);
            assignmentSources = assignments.bindSources(tableInfo, params, subQueryResults);
            updateColumnNames = assignments.targetNames();
        }

        InputRow insertValues = new InputRow(insertInputs);
        Function<String, ShardUpsertRequest.Item> itemFactory = id -> new ShardUpsertRequest.Item(
            id,
            assignmentSources,
            insertValues.materialize(),
            null, null, null);

        RowShardResolver rowShardResolver = new RowShardResolver(
            plannerContext.transactionContext(),
            plannerContext.functions(),
            writerProjection.primaryKeys(),
            writerProjection.ids(),
            writerProjection.clusteredByIdent(),
            writerProjection.clusteredBy());

        var indexNameResolver = IndexNameResolver.create(
            writerProjection.tableIdent(),
            writerProjection.partitionIdent(),
            partitionedByInputs);

        GroupRowsByShard<ShardUpsertRequest, ShardUpsertRequest.Item> grouper = new GroupRowsByShard<>(
            dependencies.clusterService(),
            rowShardResolver,
            new TypeGuessEstimateRowSize(),
            IndexNameResolver.create(
                writerProjection.tableIdent(),
                writerProjection.partitionIdent(),
                partitionedByInputs),
            context.expressions(),
            itemFactory,
            true);

        List<Row> rows = StreamSupport.stream(
            evaluateValueTableFunction(
                tableFunctionRelation.functionImplementation(),
                tableFunctionRelation.function().arguments(),
                writerProjection.allTargetReferences(),
                tableInfo,
                params,
                plannerContext,
                subQueryResults).spliterator(), false)
            .collect(toList());

        ShardUpsertRequest.Builder builder = new ShardUpsertRequest.Builder(
            plannerContext.transactionContext().sessionSettings(),
            BULK_REQUEST_TIMEOUT_SETTING.setting().get(dependencies.settings()),
            writerProjection.isIgnoreDuplicateKeys()
                ? ShardUpsertRequest.DuplicateKeyAction.IGNORE
                : ShardUpsertRequest.DuplicateKeyAction.UPDATE_OR_FAIL,
            rows.size() > 1, // continueOnErrors
            updateColumnNames,
            writerProjection.allTargetReferences().toArray(new Reference[0]),
            plannerContext.jobId(),
            false);
        var shardedRequests = new ShardedRequests<>(builder::newRequest);

        HashMap<String, InsertSourceFromCells> sourceValidators = new HashMap<>();
        for (Row row : rows) {
            grouper.accept(shardedRequests, row);

            for (var key : primaryKeys) {
                if (key.value() == null) {
                    throw new IllegalArgumentException("Primary key value must not be NULL");
                }
            }
            if (clusterByInput != null && clusterByInput.value() == null) {
                throw new IllegalArgumentException("Clustered by value must not be NULL");
            }

            String indexName = indexNameResolver.get();
            var validator = sourceValidators.computeIfAbsent(
                indexName,
                index -> new InsertSourceFromCells(
                    plannerContext.transactionContext(),
                    plannerContext.functions(),
                    tableInfo,
                    index,
                    GeneratedColumns.Validation.VALUE_MATCH,
                    writerProjection.allTargetReferences(),
                    true));

            var cells = row.materialize();
            try {
                validator.generateSourceAndCheckConstraints(cells);
            } catch (Throwable t) {
                consumer.accept(null, t);
            }
        }
        sourceValidators.clear();

        var actionProvider = dependencies.transportActionProvider();
        createIndices(
            actionProvider.transportBulkCreateIndicesAction(),
            shardedRequests.itemsByMissingIndex().keySet(),
            dependencies.clusterService(),
            plannerContext.jobId()
        ).thenCompose(acknowledgedResponse -> {
            var shardUpsertRequests = resolveAndGroupShardRequests(
                shardedRequests,
                dependencies.clusterService()).values();
            return execute(
                shardUpsertRequests,
                actionProvider.transportShardUpsertAction(),
                dependencies.scheduler());
        }).whenComplete((response, t) -> {
            if (t == null) {
                consumer.accept(
                    InMemoryBatchIterator.of(new Row1((long) response.numSuccessfulWrites()), SENTINEL), null);
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
        String[] updateColumnNames;
        Assignments assignments;
        if (writerProjection.onDuplicateKeyAssignments() == null) {
            assignments = null;
            updateColumnNames = null;
        } else {
            assignments = Assignments.convert(writerProjection.onDuplicateKeyAssignments());
            updateColumnNames = assignments.targetNames();
        }

        ShardUpsertRequest.Builder builder = new ShardUpsertRequest.Builder(
            plannerContext.transactionContext().sessionSettings(),
            BULK_REQUEST_TIMEOUT_SETTING.setting().get(dependencies.settings()),
            writerProjection.isIgnoreDuplicateKeys()
                ? ShardUpsertRequest.DuplicateKeyAction.IGNORE
                : ShardUpsertRequest.DuplicateKeyAction.UPDATE_OR_FAIL,
            true, // continueOnErrors
            updateColumnNames,
            writerProjection.allTargetReferences().toArray(new Reference[0]),
            plannerContext.jobId(),
            true);
        var shardedRequests = new ShardedRequests<>(builder::newRequest);

        DocTableInfo tableInfo = dependencies
            .schemas()
            .getTableInfo(
                RelationName.fromIndexName(writerProjection.tableIdent().fqn()),
                Operation.INSERT);

        InputFactory inputFactory = new InputFactory(dependencies.functions());
        InputFactory.Context<CollectExpression<Row, ?>> context =
            inputFactory.ctxForInputColumns(plannerContext.transactionContext());

        var columnSymbols = InputColumns.create(
            writerProjection.allTargetReferences(),
            new InputColumns.SourceSymbols(writerProjection.allTargetReferences()));
        ArrayList<Input<?>> insertInputs = new ArrayList<>(columnSymbols.size());
        for (Symbol symbol : columnSymbols) {
            insertInputs.add(context.add(symbol));
        }
        ArrayList<Input<?>> partitionedByInputs = new ArrayList<>(writerProjection.partitionedBySymbols().size());
        for (Symbol partitionedBySymbol : writerProjection.partitionedBySymbols()) {
            partitionedByInputs.add(context.add(partitionedBySymbol));
        }
        ArrayList<Input<?>> primaryKeys = new ArrayList<>(writerProjection.columnSymbols().size());
        for (Symbol symbol : writerProjection.ids()) {
            primaryKeys.add(context.add(symbol));
        }
        Input<?> clusterByInput;
        if (writerProjection.clusteredBy() != null) {
            clusterByInput = context.add(writerProjection.clusteredBy());
        } else {
            clusterByInput = null;
        }

        for (Symbol symbol : writerProjection.ids()) {
            primaryKeys.add(context.add(symbol));
        }

        var indexNameResolver = IndexNameResolver.create(
            writerProjection.tableIdent(),
            writerProjection.partitionIdent(),
            partitionedByInputs);

        HashMap<String, InsertSourceFromCells> sourceValidators = new HashMap<>();
        IntArrayList bulkIndices = new IntArrayList();
        for (int bulkIdx = 0; bulkIdx < bulkParams.size(); bulkIdx++) {
            Row param = bulkParams.get(bulkIdx);

            final Symbol[] assignmentSources;
            if (assignments != null) {
                assignmentSources = assignments.bindSources(tableInfo, param, subQueryResults);
            } else {
                assignmentSources = null;
            }

            InputRow insertValues = new InputRow(insertInputs);
            Function<String, ShardUpsertRequest.Item> itemFactory = id -> new ShardUpsertRequest.Item(
                id,
                assignmentSources,
                insertValues.materialize(),
                null, null, null);

            RowShardResolver rowShardResolver = new RowShardResolver(
                plannerContext.transactionContext(),
                plannerContext.functions(),
                writerProjection.primaryKeys(),
                writerProjection.ids(),
                writerProjection.clusteredByIdent(),
                writerProjection.clusteredBy());

            GroupRowsByShard<ShardUpsertRequest, ShardUpsertRequest.Item> grouper = new GroupRowsByShard<>(
                dependencies.clusterService(),
                rowShardResolver,
                new TypeGuessEstimateRowSize(),
                indexNameResolver,
                context.expressions(),
                itemFactory,
                true);

            Iterable<Row> rows = evaluateValueTableFunction(
                tableFunctionRelation.functionImplementation(),
                tableFunctionRelation.function().arguments(),
                writerProjection.allTargetReferences(),
                tableInfo,
                param,
                plannerContext,
                subQueryResults);

            for (Row row : rows) {
                grouper.accept(shardedRequests, row);

                for (var key : primaryKeys) {
                    if (key.value() == null) {
                        throw new IllegalArgumentException("Primary key value must not be NULL");
                    }
                }
                if (clusterByInput != null && clusterByInput.value() == null) {
                    throw new IllegalArgumentException("Clustered by value must not be NULL");
                }
                String indexName = indexNameResolver.get();
                var validator = sourceValidators.computeIfAbsent(
                    indexName,
                    index -> new InsertSourceFromCells(
                        plannerContext.transactionContext(),
                        plannerContext.functions(),
                        tableInfo,
                        index,
                        GeneratedColumns.Validation.VALUE_MATCH,
                        writerProjection.allTargetReferences(),
                        true));

                var cells = row.materialize();
                try {
                    validator.generateSourceAndCheckConstraints(cells);
                } catch (Throwable t) {
                    return List.of(CompletableFuture.failedFuture(t));
                }
                bulkIndices.add(bulkIdx);
            }
        }
        sourceValidators.clear();

        List<CompletableFuture<Long>> results = createUnsetFutures(bulkParams.size());
        var actionProvider = dependencies.transportActionProvider();
        createIndices(
            actionProvider.transportBulkCreateIndicesAction(),
            shardedRequests.itemsByMissingIndex().keySet(),
            dependencies.clusterService(), plannerContext.jobId()
        ).thenCompose(acknowledgedResponse -> {
            var shardUpsertRequests = resolveAndGroupShardRequests(
                shardedRequests,
                dependencies.clusterService()).values();
            return execute(
                shardUpsertRequests,
                actionProvider.transportShardUpsertAction(),
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

    private static Iterable<Row> evaluateValueTableFunction(TableFunctionImplementation<?> funcImplementation,
                                                            List<Symbol> arguments,
                                                            List<Reference> allTargetReferences,
                                                            DocTableInfo tableInfo,
                                                            Row params,
                                                            PlannerContext plannerContext,
                                                            SubQueryResults subQueryResults) {
        SymbolEvaluator symbolEval = new SymbolEvaluator(
            plannerContext.transactionContext(),
            plannerContext.functions(),
            subQueryResults);
        Function<? super Symbol, Input<?>> eval = (symbol) -> symbol.accept(symbolEval, params);

        ArrayList<Input<?>> boundArguments = new ArrayList<>(arguments.size());
        for (int i = 0; i < arguments.size(); i++) {
            boundArguments.add(eval.apply(arguments.get(i)));
        }
        //noinspection unchecked
        Iterable<Row> rows = funcImplementation.evaluate(
            plannerContext.transactionContext(),
            boundArguments.toArray(new Input[0]));
        return Iterables.transform(rows, row -> cast(row, allTargetReferences, tableInfo));
    }

    private static Row cast(Row row, List<Reference> columnReferences, DocTableInfo tableInfo) {
        if (row == null) {
            return null;
        }
        Object[] cells = new Object[row.numColumns()];
        for (int i = 0; i < cells.length; i++) {
            Reference reference = columnReferences.get(i);
            DataType<?> targetType = reference.valueType();
            Object value = row.get(i);
            try {
                cells[i] = targetType.value(value);
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
                shardedRequests.add(itemAndRoutingAndSourceInfo.item(), 0, shardLocation, null);
                requestItemsIterator.remove();
            }
            if (requestItems.isEmpty()) {
                itemsByMissingIndex.remove();
            }
        }

        return shardedRequests.itemsByShard();
    }

    private CompletableFuture<ShardResponse.CompressedResult> execute(Collection<ShardUpsertRequest> shardUpsertRequests,
                                                                      TransportShardUpsertAction shardUpsertAction,
                                                                      ScheduledExecutorService scheduler) {
        ShardResponse.CompressedResult compressedResult = new ShardResponse.CompressedResult();
        if (shardUpsertRequests.isEmpty()) {
            return CompletableFuture.completedFuture(compressedResult);
        }

        CompletableFuture<ShardResponse.CompressedResult> result = new CompletableFuture<>();
        AtomicInteger numRequests = new AtomicInteger(shardUpsertRequests.size());
        AtomicReference<Throwable> lastFailure = new AtomicReference<>(null);

        for (ShardUpsertRequest request : shardUpsertRequests) {

            ActionListener<ShardResponse> listener = new ActionListener<>() {
                @Override
                public void onResponse(ShardResponse shardResponse) {
                    Throwable throwable = shardResponse.failure();
                    if (throwable == null) {
                        synchronized (compressedResult) {
                            compressedResult.update(shardResponse);
                        }
                    } else {
                        lastFailure.set(throwable);
                    }
                    countdown();
                }

                @Override
                public void onFailure(Exception e) {
                    if (!partitionWasDeleted(e, request.index())) {
                        synchronized (compressedResult) {
                            compressedResult.markAsFailed(request.items());
                        }
                    }
                    lastFailure.set(e);
                    countdown();
                }

                private void countdown() {
                    if (numRequests.decrementAndGet() == 0) {
                        Throwable throwable = lastFailure.get();
                        if (throwable == null) {
                            result.complete(compressedResult);
                        } else {
                            throwable = SQLExceptions.unwrap(throwable, t -> t instanceof RuntimeException);
                            // we want to report duplicate key exceptions
                            if (!SQLExceptions.isDocumentAlreadyExistsException(throwable) &&
                                (partitionWasDeleted(throwable, request.index())
                                 || mixedArgumentTypesFailure(throwable))) {
                                result.complete(compressedResult);
                            } else {
                                result.completeExceptionally(throwable);
                            }
                        }
                    }
                }
            };

            shardUpsertAction.execute(
                request,
                new RetryListener<>(
                    scheduler,
                    l -> shardUpsertAction.execute(request, l),
                    listener,
                    BACK_OFF_POLICY
                )
            );
        }
        return result;
    }

    private static boolean mixedArgumentTypesFailure(Throwable throwable) {
        return throwable instanceof ClassCastException
               || throwable instanceof NotSerializableExceptionWrapper;
    }

    private static boolean partitionWasDeleted(Throwable throwable, String index) {
        return throwable instanceof IndexNotFoundException && IndexParts.isPartitioned(index);
    }

    private static CompletableFuture<AcknowledgedResponse> createIndices(TransportCreatePartitionsAction
                                                                             createPartitionsAction,
                                                                         Set<String> indices,
                                                                         ClusterService clusterService,
                                                                         UUID jobId) {
        MetaData metaData = clusterService.state().getMetaData();
        List<String> indicesToCreate = new ArrayList<>();
        for (var index : indices) {
            if (IndexParts.isPartitioned(index) && metaData.hasIndex(index) == false) {
                indicesToCreate.add(index);
            }
        }

        FutureActionListener<AcknowledgedResponse, AcknowledgedResponse> listener = new FutureActionListener<>(r -> r);
        createPartitionsAction.execute(new CreatePartitionsRequest(indicesToCreate, jobId), listener);
        return listener;
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
    public ExecutionPlan build(PlannerContext plannerContext,
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
    public List<AbstractTableRelation> baseTables() {
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
    public Map<LogicalPlan, SelectSymbol> dependencies() {
        return Map.of();
    }

    @Override
    public long numExpectedRows() {
        return -1L;
    }

    @Override
    public long estimatedRowSize() {
        return 0L;
    }

    @Override
    public <C, R> R accept(LogicalPlanVisitor<C, R> visitor, C context) {
        return visitor.visitInsert(this, context);
    }
}
