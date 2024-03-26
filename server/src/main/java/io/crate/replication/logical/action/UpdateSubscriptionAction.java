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

import java.io.IOException;
import java.util.HashMap;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import org.jetbrains.annotations.VisibleForTesting;
import io.crate.metadata.RelationName;
import io.crate.replication.logical.exceptions.SubscriptionUnknownException;
import io.crate.replication.logical.metadata.Subscription;
import io.crate.replication.logical.metadata.SubscriptionsMetadata;

public class UpdateSubscriptionAction extends ActionType<AcknowledgedResponse> {

    public static final String NAME = "internal:crate:replication/logical/subscription/update";
    public static final UpdateSubscriptionAction INSTANCE = new UpdateSubscriptionAction();

    public UpdateSubscriptionAction() {
        super(NAME);
    }

    @Override
    public Writeable.Reader<AcknowledgedResponse> getResponseReader() {
        return AcknowledgedResponse::new;
    }

    @VisibleForTesting
    static Subscription updateSubscription(Subscription oldSubscription, Subscription newSubscription) {
        HashMap<RelationName, Subscription.RelationState> relations = new HashMap<>();
        for (var entry : newSubscription.relations().entrySet()) {
            var relationName = entry.getKey();
            relations.put(
                relationName,
                Subscription.updateRelationState(
                    oldSubscription.relations().get(relationName),
                    entry.getValue()
                )
            );
        }
        return new Subscription(
            newSubscription.owner(),
            newSubscription.connectionInfo(),
            newSubscription.publications(),
            newSubscription.settings(),
            relations
        );
    }

    public static ClusterState update(ClusterState clusterState,
                                      String subscriptionName,
                                      Subscription subscription) {
        Metadata currentMetadata = clusterState.metadata();
        Metadata.Builder mdBuilder = Metadata.builder(currentMetadata);

        var oldMetadata = (SubscriptionsMetadata) mdBuilder.getCustom(SubscriptionsMetadata.TYPE);
        if (oldMetadata == null || oldMetadata.subscription().containsKey(subscriptionName) == false) {
            throw new SubscriptionUnknownException(subscriptionName);
        }

        var oldSubscription = oldMetadata.subscription().get(subscriptionName);
        var newSubscription = updateSubscription(oldSubscription, subscription);

        if (oldSubscription.equals(newSubscription)) {
            return clusterState;
        }

        var newMetadata = SubscriptionsMetadata.newInstance(oldMetadata);
        newMetadata.subscription().put(subscriptionName, newSubscription);
        assert !newMetadata.equals(oldMetadata) : "must not be equal to guarantee the cluster change action";
        mdBuilder.putCustom(SubscriptionsMetadata.TYPE, newMetadata);

        return ClusterState.builder(clusterState).metadata(mdBuilder).build();
    }

    @Singleton
    public static class TransportAction extends TransportMasterNodeAction<Request, AcknowledgedResponse> {

        @Inject
        public TransportAction(TransportService transportService,
                               ClusterService clusterService,
                               ThreadPool threadPool) {
            super(NAME,
                  transportService,
                  clusterService,
                  threadPool,
                  Request::new);
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
        protected void masterOperation(Request request,
                                       ClusterState state,
                                       ActionListener<AcknowledgedResponse> listener) throws Exception {
            clusterService.submitStateUpdateTask(
                "update-subscription",
                new AckedClusterStateUpdateTask<>(request, listener) {
                    @Override
                    public ClusterState execute(ClusterState currentState) throws Exception {
                        return update(currentState, request.subscriptionName, request.subscription);
                    }

                    @Override
                    protected AcknowledgedResponse newResponse(boolean acknowledged) {
                        return new AcknowledgedResponse(acknowledged);
                    }
                }
            );

        }

        @Override
        protected ClusterBlockException checkBlock(Request request,
                                                   ClusterState state) {
            return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
        }
    }


    public static class Request extends AcknowledgedRequest<Request> {

        private final String subscriptionName;
        private final Subscription subscription;

        public Request(String subscriptionName, Subscription subscription) {
            this.subscriptionName = subscriptionName;
            this.subscription = subscription;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            subscriptionName = in.readString();
            subscription = new Subscription(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(subscriptionName);
            subscription.writeTo(out);
        }
    }
}
