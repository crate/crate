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

package io.crate.statistics;

import io.crate.Streamer;
import io.crate.action.FutureActionListener;
import io.crate.concurrent.CompletableFutures;
import io.crate.data.Row;
import io.crate.execution.ddl.AnalyzeRequest;
import io.crate.execution.support.MultiActionListener;
import io.crate.execution.support.NodeActionRequestHandler;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.util.concurrent.CompletableFuture.completedFuture;

@Singleton
public final class TransportAnalyzeAction {

    private static final Logger LOGGER = LogManager.getLogger(TransportAnalyzeAction.class);
    private static final String INVOKE_ANALYZE = "internal:crate:sql/analyze/invoke";
    private static final String FETCH_SAMPLES = "internal:crate:sql/analyze/fetch_samples";
    private static final String RECEIVE_TABLE_STATS = "internal:crate:sql/analyze/receive_stats";

    /**
     * This number is from PostgreSQL, they chose this based on the paper
     * "Random sampling for histogram construction: how much is enough?"
     *
     * > Their Corollary 1 to Theorem 5 says that for table size n, histogram size k,
     * > maximum relative error in bin size f, and error probability gamma, the minimum random sample size is
     * >    r = 4 * k * ln(2*n/gamma) / f^2
     * > Taking f = 0.5, gamma = 0.01, n = 10^6 rows, we obtain r = 305.82 * k
     * > Note that because of the log function, the dependence on n is quite weak;
     * > even at n = 10^12, a 300*k sample gives <= 0.66 bin size error with probability 0.99.
     * > So there's no real need to scale for n, which is a good thing because we don't necessarily know it at this point.
     *
     * In PostgreSQL `k` is configurable (per column). We don't support changing k, we default it to 100
     */
    private static final int NUM_SAMPLES = 300 * MostCommonValues.MCV_TARGET;
    private final TransportService transportService;
    private final Schemas schemas;
    private final ClusterService clusterService;

    @Inject
    public TransportAnalyzeAction(TransportService transportService,
                                  ReservoirSampler reservoirSampler,
                                  Schemas schemas,
                                  ClusterService clusterService,
                                  TableStats tableStats) {
        this.transportService = transportService;
        this.schemas = schemas;
        this.clusterService = clusterService;
        transportService.registerRequestHandler(
            INVOKE_ANALYZE,
            AnalyzeRequest::new,
            ThreadPool.Names.SAME, // goes async right away
            new NodeActionRequestHandler<>(req -> fetchSamplesThenGenerateAndPublishStats())
        );
        transportService.registerRequestHandler(
            FETCH_SAMPLES,
            FetchSampleRequest::new,
            ThreadPool.Names.SEARCH,
            new NodeActionRequestHandler<>(
                req -> completedFuture(new FetchSampleResponse(
                    reservoirSampler.getSamples(req.relation(), req.columns(), req.maxSamples())))
            )
        );
        transportService.registerRequestHandler(
            RECEIVE_TABLE_STATS,
            PublishTableStatsRequest::new,
            ThreadPool.Names.SAME, // cheap operation
            new NodeActionRequestHandler<>(
                req -> {
                    tableStats.updateTableStats(req.tableStats());
                    return completedFuture(new AcknowledgedResponse(true));
                }
            )
        );
    }

    public CompletableFuture<AcknowledgedResponse> fetchSamplesThenGenerateAndPublishStats() {
        ArrayList<CompletableFuture<Map.Entry<RelationName, Stats>>> futures = new ArrayList<>();
        for (SchemaInfo schema : schemas) {
            if (!(schema instanceof DocSchemaInfo)) {
                continue;
            }
            for (TableInfo table : schema.getTables()) {
                List<Reference> primitiveColumns = StreamSupport.stream(table.spliterator(), false)
                    .filter(x -> !x.column().isSystemColumn())
                    .filter(x -> DataTypes.PRIMITIVE_TYPES.contains(x.valueType()))
                    .collect(Collectors.toList());

                futures.add(fetchSamples(
                    table.ident(),
                    primitiveColumns
                ).thenApply(samples -> Map.entry(table.ident(), createTableStats(samples, primitiveColumns))));
            }
        }
        return CompletableFutures.allAsList(futures)
            .thenApply(entries -> Map.ofEntries(entries.toArray(new Map.Entry[0])))
            .thenCompose(entries -> publishTableStats((Map<RelationName, Stats>)(Map) entries));
    }

    private CompletableFuture<AcknowledgedResponse> publishTableStats(Map<RelationName, Stats> newTableStats) {
        List<DiscoveryNode> nodesOn41OrAfter = StreamSupport.stream(clusterService.state().nodes().spliterator(), false)
            .filter(x -> x.getVersion().onOrAfter(Version.V_4_1_0))
            .collect(Collectors.toList());
        var listener = new FutureActionListener<AcknowledgedResponse, AcknowledgedResponse>(x -> x);
        var multiListener = new MultiActionListener<>(
            nodesOn41OrAfter.size(),
            Collectors.reducing(
                new AcknowledgedResponse(true),
                (resp1, resp2) -> new AcknowledgedResponse(resp1.isAcknowledged() && resp2.isAcknowledged())
            ),
            listener
        );
        var responseHandler = new ActionListenerResponseHandler<>(
            multiListener,
            AcknowledgedResponse::new,
            ThreadPool.Names.SAME
        );
        PublishTableStatsRequest request = new PublishTableStatsRequest(newTableStats);
        for (DiscoveryNode node : nodesOn41OrAfter) {
            transportService.sendRequest(node, RECEIVE_TABLE_STATS, request, responseHandler);
        }
        return listener;
    }

    private static Stats createTableStats(Samples samples, List<Reference> primitiveColumns) {
        List<Row> records = samples.records;
        List<Object> columnValues = new ArrayList<>(records.size());
        Map<ColumnIdent, ColumnStats> statsByColumn = new HashMap<>(primitiveColumns.size());
        for (int i = 0; i < primitiveColumns.size(); i++) {
            Reference primitiveColumn = primitiveColumns.get(i);
            columnValues.clear();
            int nullCount = 0;
            for (Row record : records) {
                Object value = record.get(i);
                if (value == null) {
                    nullCount++;
                } else {
                    columnValues.add(value);
                }
            }
            @SuppressWarnings("unchecked")
            DataType<Object> dataType = (DataType<Object>) primitiveColumn.valueType();
            columnValues.sort(dataType::compare);
            ColumnStats<?> columnStats = ColumnStats.fromSortedValues(
                columnValues,
                dataType,
                nullCount,
                samples.numTotalDocs
            );
            statsByColumn.put(primitiveColumn.column(), columnStats);
        }
        return new Stats(samples.numTotalDocs, samples.numTotalSizeInBytes, statsByColumn);
    }

    private CompletableFuture<Samples> fetchSamples(RelationName relationName, List<Reference> columns) {
        FutureActionListener<FetchSampleResponse, Samples> listener = new FutureActionListener<>(FetchSampleResponse::samples);
        List<DiscoveryNode> nodesOn41OrAfter = StreamSupport.stream(clusterService.state().nodes().spliterator(), false)
            .filter(x -> x.getVersion().onOrAfter(Version.V_4_1_0))
            .collect(Collectors.toList());
        MultiActionListener<FetchSampleResponse, ?, FetchSampleResponse> multiListener = new MultiActionListener<>(
            nodesOn41OrAfter.size(),
            Collectors.reducing(
                new FetchSampleResponse(Samples.EMPTY),
                (FetchSampleResponse s1, FetchSampleResponse s2) -> FetchSampleResponse.merge(TransportAnalyzeAction.NUM_SAMPLES, s1, s2)),
            listener
        );
        List<Streamer> streamers = Arrays.asList(Symbols.streamerArray(columns));
        ActionListenerResponseHandler<FetchSampleResponse> responseHandler = new ActionListenerResponseHandler<>(
            multiListener,
            in -> new FetchSampleResponse(streamers, in),
            ThreadPool.Names.SAME
        );
        for (DiscoveryNode node : nodesOn41OrAfter) {
            transportService.sendRequest(
                node,
                FETCH_SAMPLES,
                new FetchSampleRequest(relationName, columns, TransportAnalyzeAction.NUM_SAMPLES),
                responseHandler
            );
        }
        return listener;
    }
}
