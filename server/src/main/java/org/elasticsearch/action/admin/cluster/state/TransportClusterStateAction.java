/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.cluster.state;

import java.io.IOException;
import java.util.function.Predicate;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.Metadata.Custom;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;

import io.crate.common.annotations.VisibleForTesting;
import io.crate.common.unit.TimeValue;

public class TransportClusterStateAction extends TransportMasterNodeReadAction<ClusterStateRequest, ClusterStateResponse> {


    @Inject
    public TransportClusterStateAction(Settings settings, TransportService transportService, ClusterService clusterService, ThreadPool threadPool) {
        super(settings, ClusterStateAction.NAME, false, transportService, clusterService, threadPool, ClusterStateRequest::new);
    }

    @Override
    protected String executor() {
        // very lightweight operation in memory, no need to fork to a thread
        return ThreadPool.Names.SAME;
    }

    @Override
    protected ClusterBlockException checkBlock(ClusterStateRequest request, ClusterState state) {
        // cluster state calls are done also on a fully blocked cluster to figure out what is going
        // on in the cluster. For example, which nodes have joined yet the recovery has not yet kicked
        // in, we need to make sure we allow those calls
        // return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA);
        return null;
    }

    @Override
    protected ClusterStateResponse read(StreamInput in) throws IOException {
        return new ClusterStateResponse(in);
    }

    @Override
    protected void masterOperation(final ClusterStateRequest request,
                                   final ClusterState state,
                                   final ActionListener<ClusterStateResponse> listener) throws IOException {

        final Predicate<ClusterState> acceptableClusterStatePredicate
            = request.waitForMetadataVersion() == null ? clusterState -> true
            : clusterState -> clusterState.metadata().version() >= request.waitForMetadataVersion();

        final Predicate<ClusterState> acceptableClusterStateOrNotMasterPredicate = request.local()
            ? acceptableClusterStatePredicate
            : acceptableClusterStatePredicate.or(clusterState -> clusterState.nodes().isLocalNodeElectedMaster() == false);

        if (acceptableClusterStatePredicate.test(state)) {
            ActionListener.completeWith(listener, () -> buildResponse(
                request,
                state,
                logger
            ));
        } else {
            assert acceptableClusterStateOrNotMasterPredicate.test(state) == false;
            new ClusterStateObserver(state, clusterService, request.waitForTimeout(), logger)
                .waitForNextChange(new ClusterStateObserver.Listener() {

                    @Override
                    public void onNewClusterState(ClusterState newState) {
                        if (acceptableClusterStatePredicate.test(newState)) {
                            ActionListener.completeWith(listener, () -> buildResponse(
                                request,
                                newState,
                                logger
                            ));
                        } else {
                            listener.onFailure(new NotMasterException(
                                "master stepped down waiting for metadata version " +
                                request.waitForMetadataVersion()));
                        }
                    }

                    @Override
                    public void onClusterServiceClose() {
                        listener.onFailure(new NodeClosedException(clusterService.localNode()));
                    }

                    @Override
                    public void onTimeout(TimeValue timeout) {
                        try {
                            listener.onResponse(new ClusterStateResponse(state.getClusterName(), null, true));
                        } catch (Exception e) {
                            listener.onFailure(e);
                        }
                    }
                }, acceptableClusterStateOrNotMasterPredicate);
        }
    }

    @VisibleForTesting
    static ClusterStateResponse buildResponse(final ClusterStateRequest request,
                                              final ClusterState currentState,
                                              final Logger logger) {
        logger.trace("Serving cluster state request using version {}", currentState.version());
        ClusterState.Builder builder = ClusterState.builder(currentState.getClusterName());
        builder.version(currentState.version());
        builder.stateUUID(currentState.stateUUID());
        if (request.nodes()) {
            builder.nodes(currentState.nodes());
        }
        if (request.routingTable()) {
            if (request.indices().length > 0) {
                RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
                String[] indices = IndexNameExpressionResolver.concreteIndexNames(currentState, request);
                for (String filteredIndex : indices) {
                    if (currentState.routingTable().indicesRouting().containsKey(filteredIndex)) {
                        routingTableBuilder.add(currentState.routingTable().indicesRouting().get(filteredIndex));
                    }
                }
                builder.routingTable(routingTableBuilder.build());
            } else {
                builder.routingTable(currentState.routingTable());
            }
        }
        if (request.blocks()) {
            builder.blocks(currentState.blocks());
        }

        Metadata.Builder mdBuilder = Metadata.builder();
        mdBuilder.clusterUUID(currentState.metadata().clusterUUID());

        if (request.metadata()) {
            if (request.indices().length == 0 && request.templates().length == 0) {
                mdBuilder = Metadata.builder(currentState.metadata());
            } else {
                if (request.indices().length > 0) {
                    String[] indices = IndexNameExpressionResolver.concreteIndexNames(currentState, request);
                    for (String filteredIndex : indices) {
                        IndexMetadata indexMetadata = currentState.metadata().index(filteredIndex);
                        if (indexMetadata != null) {
                            mdBuilder.put(indexMetadata, false);
                        }
                    }
                }
                for (String template : request.templates()) {
                    var templateMetadata = currentState.metadata().templates().get(template);
                    if (templateMetadata != null) {
                        mdBuilder.put(templateMetadata);
                    }
                }
            }

            // filter out metadata that shouldn't be returned by the API
            for (ObjectObjectCursor<String, Custom> custom : currentState.metadata().customs()) {
                if (custom.value.context().contains(Metadata.XContentContext.API) == false) {
                    mdBuilder.removeCustom(custom.key);
                }
            }
        }
        builder.metadata(mdBuilder);

        if (request.customs()) {
            for (ObjectObjectCursor<String, ClusterState.Custom> custom : currentState.customs()) {
                if (custom.value.isPrivate() == false) {
                    builder.putCustom(custom.key, custom.value);
                }
            }
        }
        return new ClusterStateResponse(currentState.getClusterName(), builder.build(), false);
    }


}
