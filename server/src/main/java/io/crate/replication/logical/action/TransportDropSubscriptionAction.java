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

import io.crate.execution.ddl.AbstractDDLTransportAction;
import io.crate.metadata.cluster.DDLClusterStateTaskExecutor;
import io.crate.replication.logical.exceptions.SubscriptionUnknownException;
import io.crate.replication.logical.metadata.SubscriptionsMetadata;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import javax.annotation.Nonnull;
import java.util.Iterator;

import static io.crate.replication.logical.LogicalReplicationSettings.REPLICATION_SUBSCRIPTION_NAME;

@Singleton
public class TransportDropSubscriptionAction extends AbstractDDLTransportAction<DropSubscriptionRequest, AcknowledgedResponse> {

    public static final String ACTION_NAME = "internal:crate:replication/logical/subscription/drop";

    @Inject
    public TransportDropSubscriptionAction(TransportService transportService,
                                           ClusterService clusterService,
                                           ThreadPool threadPool,
                                           IndexNameExpressionResolver indexNameExpressionResolver) {
        super(ACTION_NAME,
            transportService,
            clusterService,
            threadPool,
            indexNameExpressionResolver,
            DropSubscriptionRequest::new,
            AcknowledgedResponse::new,
            AcknowledgedResponse::new,
            "drop-subscription");
    }

    @Override
    public ClusterStateTaskExecutor<DropSubscriptionRequest> clusterStateTaskExecutor(DropSubscriptionRequest request) {
        return new DDLClusterStateTaskExecutor<>() {
            @Override
            protected ClusterState execute(ClusterState currentState, DropSubscriptionRequest dropSubscriptionRequest) throws Exception {
                Metadata currentMetadata = currentState.metadata();
                Metadata.Builder mdBuilder = Metadata.builder(currentMetadata);

                SubscriptionsMetadata oldMetadata = (SubscriptionsMetadata) mdBuilder.getCustom(SubscriptionsMetadata.TYPE);
                if (oldMetadata != null && oldMetadata.subscription().containsKey(request.name())) {

                    // This is second step out of 3. Tracking of shards is already stopped on the first step (CLOSE TABLE).
                    // Removing setting so that last step (OPEN TABLE) will update engine.
                    SubscriptionsMetadata newMetadata = SubscriptionsMetadata.newInstance(oldMetadata);
                    newMetadata.subscription().remove(request.name());
                    assert !newMetadata.equals(oldMetadata) : "must not be equal to guarantee the cluster change action";
                    mdBuilder.putCustom(SubscriptionsMetadata.TYPE, newMetadata);

                    mdBuilder = removeSubscriptionSetting(currentMetadata.indices(), mdBuilder, request.name());

                    return ClusterState.builder(currentState).metadata(mdBuilder).build();
                } else if (request.ifExists() == false) {
                    throw new SubscriptionUnknownException(request.name());
                }

                return currentState;
            }
        };
    }

    @Override
    protected ClusterBlockException checkBlock(DropSubscriptionRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    /**
     * Removes the REPLICATION_SUBSCRIPTION_NAME index setting from all relevant to subscription indices.
     */
    private Metadata.Builder removeSubscriptionSetting(ImmutableOpenMap<String, IndexMetadata> indices,
                                                       Metadata.Builder mdBuilder,
                                                       @Nonnull String subscriptionToDrop) {
        Iterator<IndexMetadata> indicesIterator = indices.valuesIt();
        while (indicesIterator.hasNext()) {
            IndexMetadata indexMetadata = indicesIterator.next();
            var settings = indexMetadata.getSettings();
            var subscriptionName = REPLICATION_SUBSCRIPTION_NAME.get(settings); // Can be null.
            if (subscriptionToDrop.equals(subscriptionName)) {
                var settingsBuilder = Settings.builder().put(settings);
                settingsBuilder.remove(REPLICATION_SUBSCRIPTION_NAME.getKey());
                mdBuilder.put(
                    IndexMetadata
                        .builder(indexMetadata)
                        .settingsVersion(1 + indexMetadata.getSettingsVersion())
                        .settings(settingsBuilder)
                );
            }
        }
        return mdBuilder;
    }

}
