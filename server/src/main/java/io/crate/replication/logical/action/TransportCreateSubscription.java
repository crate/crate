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
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RelationMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import io.crate.common.exceptions.Exceptions;
import io.crate.exceptions.RelationAlreadyExists;
import io.crate.metadata.RelationName;
import io.crate.replication.logical.LogicalReplicationService;
import io.crate.replication.logical.exceptions.PublicationUnknownException;
import io.crate.replication.logical.exceptions.SubscriptionAlreadyExistsException;
import io.crate.replication.logical.metadata.Subscription;
import io.crate.replication.logical.metadata.SubscriptionsMetadata;
import io.crate.role.Roles;

public class TransportCreateSubscription extends TransportMasterNodeAction<CreateSubscriptionRequest, AcknowledgedResponse> {

    public static final Action ACTION = new Action();
    private final String source;
    private final LogicalReplicationService logicalReplicationService;
    private final Roles roles;

    public static class Action extends ActionType<AcknowledgedResponse> {
        private static final String NAME = "internal:crate:replication/logical/subscription/create";

        private Action() {
            super(NAME);
        }
    }

    @Inject
    public TransportCreateSubscription(TransportService transportService,
                                       ClusterService clusterService,
                                       LogicalReplicationService logicalReplicationService,
                                       ThreadPool threadPool,
                                       Roles roles) {
        super(ACTION.name(),
              transportService,
              clusterService,
              threadPool,
              CreateSubscriptionRequest::new);
        this.logicalReplicationService = logicalReplicationService;
        this.roles = roles;
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

        // Ensure subscription owner exists
        if (roles.findUser(request.owner()) == null) {
            throw new IllegalStateException(
                String.format(
                    Locale.ENGLISH, "Subscription '%s' cannot be created as the user '%s' owning the subscription has been dropped.",
                    request.name(),
                    request.owner()
                )
            );
        }

        logicalReplicationService.getPublicationState(
                request.name(),
                request.publications(),
                request.connectionInfo()
            )
            .thenCompose(
                response -> {
                    if (response.unknownPublications().isEmpty() == false) {
                        throw new PublicationUnknownException(response.unknownPublications().get(0));
                    }

                    // Published tables can have metadata or documents which subscriber with a lower version might not process.
                    // We check published tables version and not publisher cluster's MinNodeVersion.
                    // Publisher cluster can have a higher version but contain old tables, restored from a snapshot,
                    // in this case subscription works fine.

                    Metadata metadata = state.metadata();
                    Metadata publisherMetadata = response.metadata();
                    for (RelationMetadata.Table table : publisherMetadata.relations(RelationMetadata.Table.class)) {
                        if (metadata.getRelation(table.name()) != null) {
                            var message = String.format(
                                Locale.ENGLISH,
                                "Subscription '%s' cannot be created as included relation '%s' already exists",
                                request.name(),
                                table.name()
                            );
                            throw new RelationAlreadyExists(table.name(), message);

                        }
                        checkVersionCompatibility(
                            table.name().fqn(),
                            state.nodes().getMinNodeVersion(),
                            table.settings()
                        );

                        for (Settings settings : publisherMetadata.getIndices(table.name(), List.of(), false, IndexMetadata::getSettings)) {
                            checkVersionCompatibility(
                                table.name().fqn(),
                                state.nodes().getMinNodeVersion(),
                                settings
                            );
                        }
                    }
                    return submitClusterStateTask(request, response);
                }
            )
            .whenComplete(
                (acknowledgedResponse, err) -> {
                    if (err == null) {
                        listener.onResponse(acknowledgedResponse);
                    } else {
                        listener.onFailure(Exceptions.toException(err));
                    }
                }
            );
    }

    private static void checkVersionCompatibility(String tableFqn, Version subscriberMinNodeVersion, Settings settings) {
        Version publishedTableVersion = settings.getAsVersion(IndexMetadata.SETTING_VERSION_CREATED, null);
        assert publishedTableVersion != null : "All published tables must have version created setting";
        if (subscriberMinNodeVersion.beforeMajorMinor(publishedTableVersion)) {
            throw new IllegalStateException(String.format(
                Locale.ENGLISH,
                "One of the published tables has version higher than subscriber's minimal node version." +
                " Table=%s, Table-Version=%s, Local-Minimal-Version: %s",
                tableFqn,
                publishedTableVersion,
                subscriberMinNodeVersion
            ));
        }
    }

    private CompletableFuture<AcknowledgedResponse> submitClusterStateTask(CreateSubscriptionRequest request,
                                                                           PublicationsStateAction.Response publicationsStateResponse) {

        AckedClusterStateUpdateTask<AcknowledgedResponse> task = new AckedClusterStateUpdateTask<>(request) {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                Metadata currentMetadata = currentState.metadata();
                Metadata.Builder mdBuilder = Metadata.builder(currentMetadata);

                var oldMetadata = (SubscriptionsMetadata) mdBuilder.getCustom(SubscriptionsMetadata.TYPE);
                if (oldMetadata != null && oldMetadata.subscription().containsKey(request.name())) {
                    throw new SubscriptionAlreadyExistsException(request.name());
                }

                HashMap<RelationName, Subscription.RelationState> relations = new HashMap<>();
                Metadata publisherMetadata = publicationsStateResponse.metadata();
                for (RelationMetadata.Table table : publisherMetadata.relations(RelationMetadata.Table.class)) {
                    relations.put(
                        table.name(),
                        new Subscription.RelationState(Subscription.State.INITIALIZING, null)
                    );
                }

                Subscription subscription = new Subscription(
                    request.owner(),
                    request.connectionInfo(),
                    request.publications(),
                    request.settings(),
                    relations
                );


                var newMetadata = SubscriptionsMetadata.newInstance(oldMetadata);
                newMetadata.subscription().put(request.name(), subscription);
                assert !newMetadata.equals(oldMetadata) : "must not be equal to guarantee the cluster change action";
                mdBuilder.putCustom(SubscriptionsMetadata.TYPE, newMetadata);

                return ClusterState.builder(currentState).metadata(mdBuilder).build();
            }

            @Override
            protected AcknowledgedResponse newResponse(boolean acknowledged) {
                return new AcknowledgedResponse(acknowledged);
            }
        };

        clusterService.submitStateUpdateTask(source, task);
        return task.completionFuture();
    }

    @Override
    protected ClusterBlockException checkBlock(CreateSubscriptionRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
