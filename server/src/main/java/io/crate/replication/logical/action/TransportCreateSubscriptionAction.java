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

import io.crate.replication.logical.LogicalReplicationService;
import io.crate.replication.logical.exceptions.SubscriptionAlreadyExistsException;
import io.crate.replication.logical.metadata.Subscription;
import io.crate.replication.logical.metadata.SubscriptionsMetadata;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.function.Consumer;

public class TransportCreateSubscriptionAction extends TransportMasterNodeAction<CreateSubscriptionRequest, AcknowledgedResponse> {

    public static final String ACTION_NAME = "internal:crate:replication/logical/subscription/create";

    private static final Logger LOGGER = Loggers.getLogger(TransportCreateSubscriptionAction.class);

    private final String source;
    private final LogicalReplicationService logicalReplicationService;

    @Inject
    public TransportCreateSubscriptionAction(TransportService transportService,
                                             ClusterService clusterService,
                                             LogicalReplicationService logicalReplicationService,
                                             ThreadPool threadPool,
                                             IndexNameExpressionResolver indexNameExpressionResolver) {
        super(ACTION_NAME,
              transportService,
              clusterService,
              threadPool,
              CreateSubscriptionRequest::new,
              indexNameExpressionResolver);
        this.logicalReplicationService = logicalReplicationService;
        this.source = "create-subscription";
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
    protected void masterOperation(CreateSubscriptionRequest request,
                                   ClusterState state,
                                   ActionListener<AcknowledgedResponse> listener) throws Exception {
        updateClusterState(
            request,
            newState -> {
                var metadata = (SubscriptionsMetadata) newState.metadata().custom(SubscriptionsMetadata.TYPE);
                var subscription = metadata.subscription().get(request.name());
                logicalReplicationService.replicate(request.name(), subscription, listener);
            },
            listener::onFailure
        );
    }

    @Override
    protected ClusterBlockException checkBlock(CreateSubscriptionRequest request,
                                               ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    private void updateClusterState(CreateSubscriptionRequest request,
                                    Consumer<ClusterState> onSuccess,
                                    Consumer<Exception> onFailure) {
        clusterService.submitStateUpdateTask(
            source,
            new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    Metadata currentMetadata = currentState.metadata();
                    Metadata.Builder mdBuilder = Metadata.builder(currentMetadata);

                    var oldMetadata = (SubscriptionsMetadata) mdBuilder.getCustom(SubscriptionsMetadata.TYPE);
                    if (oldMetadata != null && oldMetadata.subscription().containsKey(request.name())) {
                        throw new SubscriptionAlreadyExistsException(request.name());
                    }

                    // create a new instance of the metadata, to guarantee the cluster changed action.
                    var newMetadata = SubscriptionsMetadata.newInstance(oldMetadata);
                    newMetadata.subscription().put(request.name(), new Subscription(
                        request.owner(),
                        request.connectionInfo(),
                        request.publications(),
                        request.settings()
                    ));
                    assert !newMetadata.equals(oldMetadata) : "must not be equal to guarantee the cluster change action";
                    mdBuilder.putCustom(SubscriptionsMetadata.TYPE, newMetadata);

                    return ClusterState.builder(currentState).metadata(mdBuilder).build();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    onFailure.accept(e);
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    onSuccess.accept(newState);
                }
            }
        );
    }
}
