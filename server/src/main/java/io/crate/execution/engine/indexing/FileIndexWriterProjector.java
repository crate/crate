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

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.amazonaws.services.s3.transfer.internal.future.CompletedFuture;
import io.crate.analyze.CopyFromParserProperties;
import io.crate.common.collections.Iterables;
import io.crate.data.*;
import io.crate.execution.dsl.phases.FileUriCollectPhase;
import io.crate.execution.dsl.projection.FileIndexWriterProjection;
import io.crate.execution.engine.pipeline.MapRowUsingInputs;
import io.crate.expression.InputRow;
import io.crate.expression.reference.file.CsvColumnExtractingExpression;
import io.crate.expression.reference.file.FileParsingExpression;
import io.crate.expression.reference.file.JsonColumnExtractingExpression;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.TransactionContext;
import io.crate.operation.collect.files.CSVLineParser;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.Settings;

import io.crate.data.breaker.RamAccounting;

import io.crate.expression.InputFactory;
import io.crate.execution.dml.upsert.ShardUpsertRequest;
import io.crate.execution.dml.upsert.ShardUpsertRequest.DuplicateKeyAction;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.execution.engine.collect.RowShardResolver;
import io.crate.execution.jobs.NodeLimits;

import static io.crate.execution.engine.window.WindowFunctionBatchIterator.materializeWithSpare;

public class FileIndexWriterProjector implements Projector {

    private final ClusterService clusterService;
    private final NodeLimits nodeJobsCounter;
    private final CircuitBreaker queryCircuitBreaker;
    private final RamAccounting ramAccounting;
    private final ScheduledExecutorService scheduler;
    private final Executor executor;
    private final TransactionContext txnCtx;
    private final NodeContext nodeCtx;
    private final Settings settings;
    private final int targetTableNumShards;
    private final int targetTableNumReplicas;
    private final ElasticsearchClient elasticsearchClient;
    private final UUID jobId;
    private final FileIndexWriterProjection fileIndexWriterProjection;

    private ShardingUpsertExecutor shardingUpsertExecutor;

    private Function<Row, Object[]> materialize;

    public FileIndexWriterProjector(ClusterService clusterService,
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
                                    UUID jobId,
                                    FileIndexWriterProjection fileIndexWriterProjection) {

        this.clusterService = clusterService;
        this.nodeJobsCounter = nodeJobsCounter;
        this.queryCircuitBreaker = queryCircuitBreaker;
        this.ramAccounting = ramAccounting;
        this.scheduler = scheduler;
        this.executor = executor;
        this.txnCtx = txnCtx;
        this.nodeCtx = nodeCtx;
        this.settings = settings;
        this.targetTableNumShards = targetTableNumShards;
        this.targetTableNumReplicas = targetTableNumReplicas;
        this.elasticsearchClient = elasticsearchClient;
        this.jobId = jobId;
        this.fileIndexWriterProjection = fileIndexWriterProjection;


         this.materialize = row -> {
            // ramAccounting.accountForAndMaybeBreak(row);
             // Do we need RowAccounting here? Why IndexWriterProjection/ColumnIndexWriterProjector is not using it when materializing?
            return materializeWithSpare(row, 0);
        };

    }

    @Override
    public BatchIterator<Row> apply(BatchIterator<Row> batchIterator) {

        return CollectingBatchIterator.newInstance(
            batchIterator,
            src -> BatchIterators
                .collect(src, Collectors.mapping(materialize, Collectors.toList()))
                .thenCompose(rows -> {
                    if (shardingUpsertExecutor!= null) {
                        return shardingUpsertExecutor.apply(batchIterator);
                    } else {
                        shardingUpsertExecutor = null; // TODO: actually create it based on passed known fields (projection info) and parsed data)
                        return CompletableFuture.completedFuture(List.of());
                        // We cannot do CompletableFuture.completedFuture(rows) as we already materialized and processed it.
                        // All components further down expect Row but not materialized values.

                        // Also, looks like a wrong semantics - iterator operates with batch (plural rows) and we want only first row.
                        // It means we need to take a first row from the batch, parse it, create executor, ignore first row for CSV ot forward further if it's JSON
                    }
                }),
            batchIterator.hasLazyResultSet()
        );
//        return CollectingBatchIterator.newInstance(batchIterator, shardingUpsertExecutor, batchIterator.hasLazyResultSet());
    }

    private static CollectExpression<Row, ?> getExpression(RelationName tableIdent,
                                                           List<Reference> targetColumns,
                                                           Reference ref,
                                                           FileUriCollectPhase.InputFormat inputFormat,
                                                           CSVLineParser csvLineParser,
                                                           FileParsingExpression.RowContext rowContext) {
        return switch (inputFormat) {
            case JSON -> new JsonColumnExtractingExpression(tableIdent, targetColumns, ref.column(), rowContext);
            case CSV -> new CsvColumnExtractingExpression(tableIdent, targetColumns, ref.column(), rowContext, ref.valueType(), csvLineParser);
        };
    }
}

