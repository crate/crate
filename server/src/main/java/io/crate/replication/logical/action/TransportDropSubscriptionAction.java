/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.replication.logical.action;

import io.crate.action.FutureActionListener;
import io.crate.exceptions.Exceptions;
import io.crate.execution.ddl.tables.TransportCloseTable;
import io.crate.execution.engine.collect.sources.InformationSchemaIterables;
import io.crate.metadata.Schemas;
import io.crate.metadata.cluster.OpenTableClusterStateTaskExecutor;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.replication.logical.exceptions.SubscriptionUnknownException;
import io.crate.replication.logical.metadata.SubscriptionsMetadata;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.*;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static io.crate.replication.logical.LogicalReplicationSettings.REPLICATION_SUBSCRIPTION_NAME;
import static io.crate.metadata.cluster.AbstractOpenCloseTableClusterStateTaskExecutor.OpenCloseTable;

@Singleton
public class TransportDropSubscriptionAction extends TransportMasterNodeAction<DropSubscriptionRequest, AcknowledgedResponse> {

    public static final String ACTION_NAME = "internal:crate:replication/logical/subscription/drop";

    private final OpenTableClusterStateTaskExecutor openTableClusterStateTaskExecutor;

    private final Schemas schemas;

    private final TransportCloseTable transportCloseTable;

    @Inject
    public TransportDropSubscriptionAction(TransportService transportService,
                                           ClusterService clusterService,
                                           ThreadPool threadPool,
                                           IndexNameExpressionResolver indexNameExpressionResolver,
                                           OpenTableClusterStateTaskExecutor openTableClusterStateTaskExecutor,
                                           Schemas schemas,
                                           TransportCloseTable transportCloseTable) {
        super(ACTION_NAME,
            transportService,
            clusterService,
            threadPool,
            DropSubscriptionRequest::new,
            indexNameExpressionResolver);
        this.openTableClusterStateTaskExecutor = openTableClusterStateTaskExecutor;
        this.schemas = schemas;
        this.transportCloseTable = transportCloseTable;
    }


    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected AcknowledgedResponse read(StreamInput in) throws IOException {
        return new AcknowledgedResponse(in);
    }

    @Override
    protected void masterOperation(DropSubscriptionRequest request, ClusterState state, ActionListener<AcknowledgedResponse> listener) throws Exception {
        List<IndexMetadata> subscriptionIndices = getSubscriptionIndices(request.name(), state.metadata().indices());

        //TODO: Replace it, all relations which are replicated by a subscription have been added into the subscription metadata.
        List<OpenCloseTable> openCloseTables = getSubscriptionTables(request.name(), schemas);

        submitCloseSubscriptionsTablesTask(request, openCloseTables)
            .thenCompose(ignore -> submitDropSubscriptionTask(request, subscriptionIndices))
            .thenCompose(ignore -> submitOpenSubscriptionsTablesTask(request, openCloseTables, subscriptionIndices))
            .whenComplete(
                (ignore, err) -> {
                    if (err == null) {
                        listener.onResponse(new AcknowledgedResponse(true));
                    } else {
                        listener.onFailure(Exceptions.toRuntimeException(err));
                    }
                }
            );
    }

    @Override
    protected ClusterBlockException checkBlock(DropSubscriptionRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    /**
     * Closes tables and consequently closes all shards of the index
     * which, in turn, stops trackers and removes retention lease on the remote cluster.
     */
    private CompletableFuture<Void> submitCloseSubscriptionsTablesTask(DropSubscriptionRequest request,
                                                                       List<OpenCloseTable> openCloseTables) {
        FutureActionListener<AcknowledgedResponse, Void> future = new FutureActionListener<>(r -> null);
        transportCloseTable.closeTables(future, openCloseTables, request.ackTimeout());
        return future;
    }

    /**
     * Removes subscription from the cluster metadata and
     * removes setting LogicalReplicationSettings.REPLICATION_SUBSCRIPTION_NAME from each relevant for subscription index.
     */
    private CompletableFuture<Void> submitDropSubscriptionTask(DropSubscriptionRequest request,
                                                               List<IndexMetadata> subscriptionIndices) {

        var future = new CompletableFuture<Void>();
        // Check if we're still the elected master before sending second update task


        clusterService.submitStateUpdateTask(
            "drop-sub-remove-setting-and-metadata",
            new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {

                    if (!clusterService.state().nodes().isLocalNodeElectedMaster()) {
                        logger.error("Couldn't execute task 'drop-sub-remove-setting-and-metadata'. Please run command DROP SUBSCRIPTION again.");
                        future.completeExceptionally(new IllegalStateException("Master was re-elected, cannot execute 'drop-sub-remove-setting-and-metadata'"));
                    }

                    Metadata currentMetadata = currentState.metadata();
                    Metadata.Builder mdBuilder = Metadata.builder(currentMetadata);

                    SubscriptionsMetadata oldMetadata = (SubscriptionsMetadata) mdBuilder.getCustom(SubscriptionsMetadata.TYPE);
                    if (oldMetadata != null && oldMetadata.subscription().containsKey(request.name())) {

                        SubscriptionsMetadata newMetadata = SubscriptionsMetadata.newInstance(oldMetadata);
                        newMetadata.subscription().remove(request.name());
                        assert !newMetadata.equals(oldMetadata) : "must not be equal to guarantee the cluster change action";
                        mdBuilder.putCustom(SubscriptionsMetadata.TYPE, newMetadata);

                        // mdBuilder is mutated accordingly.
                        removeSubscriptionSetting(subscriptionIndices, mdBuilder);

                        return ClusterState.builder(currentState).metadata(mdBuilder).build();
                    } else if (request.ifExists() == false) {
                        throw new SubscriptionUnknownException(request.name());
                    }
                    return currentState;
                }

                @Override
                public void onFailure(String source, Exception e) {
                    logger.error("Couldn't execute task 'drop-sub-remove-setting-and-metadata'. Please run command DROP SUBSCRIPTION again.");
                    future.completeExceptionally(e);
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    future.complete(null);
                }
            }
        );
        return future;
        /*
        var future = new CompletableFuture<Void>();

        clusterService.submitStateUpdateTask(
            "drop-sub-remove-setting-and-metadata",
            // Highest priority to reduce probability of master re-election and necessity to re-run some commands.
            new ChainedAckedClusterStateUpdateTask(Priority.IMMEDIATE,
                request,
                future,
                clusterService,
                (currentState) ->  {
                    Metadata currentMetadata = currentState.metadata();
                    Metadata.Builder mdBuilder = Metadata.builder(currentMetadata);

                    SubscriptionsMetadata oldMetadata = (SubscriptionsMetadata) mdBuilder.getCustom(SubscriptionsMetadata.TYPE);
                    var subscription = oldMetadata.subscription().get(request.name());
                    if (oldMetadata != null && subscription != null) {

                        SubscriptionsMetadata newMetadata = SubscriptionsMetadata.newInstance(oldMetadata);
                        newMetadata.subscription().remove(request.name());
                        assert !newMetadata.equals(oldMetadata) : "must not be equal to guarantee the cluster change action";
                        mdBuilder.putCustom(SubscriptionsMetadata.TYPE, newMetadata);

                        // mdBuilder and subscriptionIndices is mutated accordingly.
                        removeSubscriptionSetting(subscriptionIndices, mdBuilder);

                        return ClusterState.builder(currentState).metadata(mdBuilder).build();
                    } else if (request.ifExists() == false) {
                        future.completeExceptionally(new SubscriptionUnknownException(request.name()));
                    }
                    return currentState;
                },
                "Please run command DROP SUBSCRIPTION again."
            )
        );
        return future;
        */

    }

    /**
     * Opens tables after removing replication setting
     * and consequently updates DocTableInfo-s with the normal engine and makes tables writable.
     */
    private CompletableFuture<Void> submitOpenSubscriptionsTablesTask(DropSubscriptionRequest request,
                                                                      List<OpenCloseTable> openCloseTables,
                                                                      List<IndexMetadata> subscriptionIndices) {

      var future = new CompletableFuture<Void>();

        clusterService.submitStateUpdateTask(
            "drop-sub-open-tables",
            new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    if (!clusterService.state().nodes().isLocalNodeElectedMaster()) {
                        logger.error("Couldn't execute task 'drop-sub-close-tables'. Please run command DROP SUBSCRIPTION again.");
                        future.completeExceptionally(new IllegalStateException("Master was re-elected, cannot execute 'drop-sub-close-tables'"));
                    }
                    return openTableClusterStateTaskExecutor.openTables(openCloseTables, subscriptionIndices, currentState);
                }

                @Override
                public void onFailure(String source, Exception e) {
                    logger.error("Couldn't execute task 'drop-sub-open-tables'. Please run command DROP SUBSCRIPTION again.");
                    future.completeExceptionally(e);
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    future.complete(null);
                }
            }
        );
        return future;

         /*
        var future = new CompletableFuture<Void>();

        clusterService.submitStateUpdateTask(
            "drop-sub-open-tables",
            // Highest priority to reduce probability of master re-election and necessity to re-run some commands.
            new ChainedAckedClusterStateUpdateTask(Priority.IMMEDIATE,
                request,
                future,
                clusterService,
                (currentState) -> openTableClusterStateTaskExecutor.openTables(openCloseTables, subscriptionIndices, currentState),
                "Please run command DROP SUBSCRIPTION again."
            )
        );
        return future;

         */
    }


    private List<IndexMetadata> getSubscriptionIndices(@Nonnull String subscriptionToDrop, ImmutableOpenMap<String, IndexMetadata> indices) {
        List<IndexMetadata> relevantIndices = new ArrayList<>();
        Iterator<IndexMetadata> indicesIterator = indices.valuesIt();
        while (indicesIterator.hasNext()) {
            IndexMetadata indexMetadata = indicesIterator.next();
            var settings = indexMetadata.getSettings();
            var subscriptionName = REPLICATION_SUBSCRIPTION_NAME.get(settings); // Can be null.
            if (subscriptionToDrop.equals(subscriptionName)) {
                relevantIndices.add(indexMetadata);
            }
        }
        return relevantIndices;
    }

    private List<OpenCloseTable> getSubscriptionTables(@Nonnull String subscriptionToDrop, Schemas schemas) {
        return InformationSchemaIterables.tablesStream(schemas)
            .filter(t -> {
                if (t instanceof DocTableInfo dt) {
                    return subscriptionToDrop.equals(REPLICATION_SUBSCRIPTION_NAME.get(dt.parameters()));
                }
                return false;
            })
            .map(dt -> new OpenCloseTable(dt.ident(), null))
            .collect(Collectors.toList());
    }

    private void removeSubscriptionSetting(List<IndexMetadata> subscriptionIndices, Metadata.Builder mdBuilder) {
        for (int i = 0;i < subscriptionIndices.size(); i++) {
            var indexMetadata = subscriptionIndices.get(i);
            var settingsBuilder = Settings.builder().put(indexMetadata.getSettings());
            settingsBuilder.remove(REPLICATION_SUBSCRIPTION_NAME.getKey());
            IndexMetadata.Builder builder = IndexMetadata
                .builder(indexMetadata)
                .settingsVersion(1 + indexMetadata.getSettingsVersion())
                .settings(settingsBuilder);
            subscriptionIndices.set(i, builder.build());
            // Important, need to update subscriptionIndices so that setting removal is visible for the next step.
            // Enhanced loop not used because it doesnt see reference update
            mdBuilder.put(indexMetadata, true);

        }
    }

}
