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
import java.util.Locale;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import io.crate.common.exceptions.Exceptions;
import io.crate.metadata.RelationName;
import io.crate.replication.logical.LogicalReplicationService;
import io.crate.replication.logical.exceptions.PublicationUnknownException;
import io.crate.replication.logical.exceptions.SubscriptionAlreadyExistsException;
import io.crate.replication.logical.metadata.RelationMetadata;
import io.crate.replication.logical.metadata.Subscription;
import io.crate.replication.logical.metadata.SubscriptionsMetadata;
import io.crate.user.UserLookup;

public class TransportCreateSubscriptionAction extends TransportMasterNodeAction<CreateSubscriptionRequest, AcknowledgedResponse> {

    public static final String ACTION_NAME = "internal:crate:replication/logical/subscription/create";

    private final String source;
    private final LogicalReplicationService logicalReplicationService;
    private final UserLookup userLookup;

    @Inject
    public TransportCreateSubscriptionAction(TransportService transportService,
                                             ClusterService clusterService,
                                             LogicalReplicationService logicalReplicationService,
                                             ThreadPool threadPool,
                                             UserLookup userLookup) {
        super(ACTION_NAME,
              transportService,
              clusterService,
              threadPool,
              CreateSubscriptionRequest::new);
        this.logicalReplicationService = logicalReplicationService;
        this.userLookup = userLookup;
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
        if (userLookup.findUser(request.owner()) == null) {
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
            .whenComplete(
                (response, err) -> {
                    if (err == null) {
                        if (response.unknownPublications().isEmpty() == false) {
                            listener.onFailure(new PublicationUnknownException(response.unknownPublications().get(0)));
                        }

                        // Published tables can have metadata or documents which subscriber with a lower version might not process.
                        // We check published tables version and not publisher cluster's MinNodeVersion.
                        // Publisher cluster can have a higher version but contain old tables, restored from a snapshot,
                        // in this case subscription works fine.
                        for (RelationMetadata relationMetadata: response.relationsInPublications().values()) {
                            if (relationMetadata.template() != null) {
                                checkVersionCompatibility(
                                    relationMetadata.name().fqn(),
                                    state.nodes().getMinNodeVersion(),
                                    relationMetadata.template().settings(),
                                    listener
                                );
                            }
                            if (!relationMetadata.indices().isEmpty()) {
                                // All indices belong to the same table and has same metadata.
                                IndexMetadata indexMetadata = relationMetadata.indices().get(0);
                                checkVersionCompatibility(
                                    relationMetadata.name().fqn(),
                                    state.nodes().getMinNodeVersion(),
                                    indexMetadata.getSettings(),
                                    listener
                                );
                            }
                        }

                        logicalReplicationService.verifyTablesDoNotExist(request.name(), response, listener);
                        submitClusterStateTask(request, response, listener);
                    } else {
                        listener.onFailure(Exceptions.toException(err));
                    }
                }
            );
    }

    private static void checkVersionCompatibility(String tableFqn,
                                                  Version subscriberMinNodeVersion,
                                                  Settings settings,
                                                  ActionListener<AcknowledgedResponse> listener) {
        Version publishedTableVersion = settings.getAsVersion(IndexMetadata.SETTING_VERSION_CREATED, null);
        assert publishedTableVersion != null : "All published tables must have version created setting";
        if (subscriberMinNodeVersion.beforeMajorMinor(publishedTableVersion)) {
            listener.onFailure(new IllegalStateException(String.format(
                Locale.ENGLISH,
                "One of the published tables has version higher than subscriber's minimal node version." +
                " Table=%s, Table-Version=%s, Local-Minimal-Version: %s",
                tableFqn,
                publishedTableVersion,
                subscriberMinNodeVersion
            )));
        }
    }

    private void submitClusterStateTask(CreateSubscriptionRequest request,
                                        PublicationsStateAction.Response publicationsStateResponse,
                                        ActionListener<AcknowledgedResponse> listener) {
        clusterService.submitStateUpdateTask(
            source,
            new AckedClusterStateUpdateTask<>(request, listener) {
                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    Metadata currentMetadata = currentState.metadata();
                    Metadata.Builder mdBuilder = Metadata.builder(currentMetadata);

                    var oldMetadata = (SubscriptionsMetadata) mdBuilder.getCustom(SubscriptionsMetadata.TYPE);
                    if (oldMetadata != null && oldMetadata.subscription().containsKey(request.name())) {
                        throw new SubscriptionAlreadyExistsException(request.name());
                    }

                    HashMap<RelationName, Subscription.RelationState> relations = new HashMap<>();
                    for (var relation : publicationsStateResponse.tables()) {
                        relations.put(
                            relation,
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
            }
        );
    }

    @Override
    protected ClusterBlockException checkBlock(CreateSubscriptionRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
