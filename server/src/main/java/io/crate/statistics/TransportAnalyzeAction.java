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

package io.crate.statistics;

import static java.util.concurrent.CompletableFuture.completedFuture;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.RelationMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import io.crate.concurrent.FutureActionListener;
import io.crate.concurrent.MultiActionListener;
import io.crate.execution.support.NodeActionRequestHandler;
import io.crate.fdw.ForeignDataWrapper;
import io.crate.fdw.ForeignDataWrappers;
import io.crate.fdw.ServersMetadata;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.settings.CoordinatorSessionSettings;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.role.Role;
import io.crate.types.DataTypes;

@Singleton
public final class TransportAnalyzeAction {

    private static final Logger LOGGER = LogManager.getLogger(TransportAnalyzeAction.class);
    private static final String FETCH_SAMPLES = "internal:crate:sql/analyze/fetch_samples";
    private static final String RECEIVE_TABLE_STATS = "internal:crate:sql/analyze/receive_stats";

    private final TransportService transportService;
    private final Schemas schemas;
    private final ClusterService clusterService;
    private final ConcurrentHashMap<FetchSampleRequest, CompletableFuture<Samples>> analysisByRequest = new ConcurrentHashMap<>();
    private final Executor executor;
    private final ForeignDataWrappers foreignDataWrappers;

    @Inject
    public TransportAnalyzeAction(TransportService transportService,
                                  ReservoirSampler reservoirSampler,
                                  NodeContext nodeContext,
                                  ClusterService clusterService,
                                  TableStatsService tableStatsService,
                                  ThreadPool threadPool,
                                  ForeignDataWrappers foreignDataWrappers) {
        this.transportService = transportService;
        this.schemas = nodeContext.schemas();
        this.clusterService = clusterService;
        this.executor = threadPool.executor(ThreadPool.Names.SEARCH);
        this.foreignDataWrappers = foreignDataWrappers;

        transportService.registerRequestHandler(
            FETCH_SAMPLES,
            ThreadPool.Names.SAME,
            FetchSampleRequest::new,
            // Explicit generic is required for eclipse JDT, otherwise it won't compile
            new NodeActionRequestHandler<FetchSampleRequest, FetchSampleResponse>(
                req -> {

                    CompletableFuture<Samples> newSamples = new CompletableFuture<>();
                    CompletableFuture<Samples> previous = analysisByRequest.putIfAbsent(
                        req,
                        newSamples
                    );

                    if (previous == null) {
                        newSamples.completeAsync(
                            () -> reservoirSampler.getSamples(req.relation(), req.tableOid(), req.columns()),
                            executor
                        );
                        return newSamples
                            .thenApply(FetchSampleResponse::new)
                            .whenComplete((res, err) -> analysisByRequest.remove(req));
                    } else {
                        return previous.thenApply(FetchSampleResponse::new);
                    }
                }
            )
        );

        transportService.registerRequestHandler(
            RECEIVE_TABLE_STATS,
            ThreadPool.Names.SAME, // cheap operation
            PublishTableStatsRequest::new,
            // Explicit generic is required for eclipse JDT, otherwise it won't compile
            new NodeActionRequestHandler<PublishTableStatsRequest, AcknowledgedResponse>(
                req -> {
                    tableStatsService.update(req.tableStats());
                    return completedFuture(new AcknowledgedResponse(true));
                }
            )
        );
    }

    public CompletableFuture<AcknowledgedResponse> fetchSamplesThenGenerateAndPublishStats() {
        LOGGER.info("ANALYZE: Start collecting samples to update table statistics");
        HashMap<RelationName, Stats> entries = new HashMap<>();
        CompletableFuture<Void> currentFetch = CompletableFuture.completedFuture(null);
        ServersMetadata servers = clusterService.state()
            .metadata()
            .custom(ServersMetadata.TYPE, ServersMetadata.EMPTY);
        TransactionContext systemTxnCtx = TransactionContext.of(CoordinatorSessionSettings.systemDefaults());
        for (SchemaInfo schema : schemas) {
            for (TableInfo table : schema.getTables()) {
                if (table instanceof DocTableInfo docTable && !docTable.isClosed()) {
                    List<Reference> primitiveColumns = StreamSupport.stream(docTable.spliterator(), false)
                        .filter(x -> !x.column().isSystemColumn())
                        .filter(x -> DataTypes.isPrimitive(x.valueType()))
                        .map(x -> docTable.getReadReference(x.column()))
                        .toList();

                    currentFetch = currentFetch
                        .thenCompose(ignored -> fetchSamples(
                            docTable.ident(),
                            docTable.oid(),
                            primitiveColumns
                        ))
                        .thenAccept(samples -> entries.put(docTable.ident(), samples.createTableStats(primitiveColumns)));
                }
            }
            
            for (RelationMetadata.ForeignTable foreignTable : schema.getForeignTables()) {
                ServersMetadata.Server server = servers.get(foreignTable.server());

                ForeignDataWrapper fdw = foreignDataWrappers.get(server.fdw());

                currentFetch = currentFetch
                    .thenCompose(ignored -> fdw.getStats(
                        Role.CRATE_USER,
                        server,
                        foreignTable,
                        systemTxnCtx,
                        List.of()
                    ))
                    .thenAccept(foreignStats -> {
                        if (foreignStats != null && !foreignStats.isEmpty()) {
                            entries.put(foreignTable.ident(), foreignStats);
                        }
                    });
            }
        }
        return currentFetch.thenCompose(ignored -> publishTableStats(entries));
    }

    private CompletableFuture<AcknowledgedResponse> publishTableStats(Map<RelationName, Stats> newTableStats) {
        LOGGER.info("ANALYZE: Finished collecting samples");
        DiscoveryNodes discoveryNodes = clusterService.state().nodes();
        var listener = new FutureActionListener<AcknowledgedResponse>();
        var multiListener = new MultiActionListener<>(
            discoveryNodes.getSize(),
            Collectors.reducing(
                new AcknowledgedResponse(true),
                (resp1, resp2) -> new AcknowledgedResponse(resp1.isAcknowledged() && resp2.isAcknowledged())
            ),
            listener
        );
        var responseHandler = new ActionListenerResponseHandler<>(
            RECEIVE_TABLE_STATS,
            multiListener,
            AcknowledgedResponse::new,
            ThreadPool.Names.SAME
        );
        PublishTableStatsRequest request = new PublishTableStatsRequest(newTableStats);
        for (DiscoveryNode node : discoveryNodes) {
            transportService.sendRequest(node, RECEIVE_TABLE_STATS, request, responseHandler);
        }
        return listener;
    }

    private CompletableFuture<Samples> fetchSamples(RelationName relationName,
                                                    int tableOid,
                                                    List<Reference> columns) {
        FutureActionListener<FetchSampleResponse> listener = new FutureActionListener<>();
        DiscoveryNodes discoveryNodes = clusterService.state().nodes();
        MultiActionListener<FetchSampleResponse, ?, FetchSampleResponse> multiListener = new MultiActionListener<>(
            discoveryNodes.getSize(),
            Collectors.reducing(
                new FetchSampleResponse(Samples.EMPTY),
                FetchSampleResponse::merge),
            listener
        );
        ActionListenerResponseHandler<FetchSampleResponse> responseHandler = new ActionListenerResponseHandler<>(
            FETCH_SAMPLES,
            multiListener,
            in -> new FetchSampleResponse(columns, in),
            ThreadPool.Names.SAME
        );
        for (DiscoveryNode node : discoveryNodes) {
            transportService.sendRequest(
                node,
                FETCH_SAMPLES,
                new FetchSampleRequest(relationName, tableOid, columns, node.getVersion()),
                responseHandler
            );
        }
        return listener.thenApply(FetchSampleResponse::samples);
    }
}
