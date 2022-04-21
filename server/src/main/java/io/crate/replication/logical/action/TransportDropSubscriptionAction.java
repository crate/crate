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
import io.crate.metadata.PartitionName;
import io.crate.metadata.cluster.DDLClusterStateTaskExecutor;
import io.crate.replication.logical.exceptions.SubscriptionUnknownException;
import io.crate.replication.logical.metadata.Subscription;
import io.crate.replication.logical.metadata.SubscriptionsMetadata;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import static io.crate.replication.logical.LogicalReplicationSettings.REPLICATION_SUBSCRIPTION_NAME;

@Singleton
public class TransportDropSubscriptionAction extends AbstractDDLTransportAction<DropSubscriptionRequest, AcknowledgedResponse> {

    public static final String ACTION_NAME = "internal:crate:replication/logical/subscription/drop";

    @Inject
    public TransportDropSubscriptionAction(TransportService transportService,
                                           ClusterService clusterService,
                                           ThreadPool threadPool) {
        super(ACTION_NAME,
            transportService,
            clusterService,
            threadPool,
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
                    var subscription = newMetadata.subscription().remove(request.name());
                    assert !newMetadata.equals(oldMetadata) : "must not be equal to guarantee the cluster change action";
                    mdBuilder.putCustom(SubscriptionsMetadata.TYPE, newMetadata);

                    removeSubscriptionSetting(subscription, currentState, mdBuilder);

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
     * Removes the REPLICATION_SUBSCRIPTION_NAME index setting from all indices included by the subscription.
     */
    private void removeSubscriptionSetting(Subscription subscription,
                                           ClusterState currentState,
                                           Metadata.Builder mdBuilder) {
        Metadata metadata = currentState.metadata();
        for (var relationName : subscription.relations().keySet()) {
            var concreteIndices = IndexNameExpressionResolver.concreteIndexNames(
                metadata,
                IndicesOptions.lenientExpandOpen(),
                relationName.indexNameOrAlias()
            );
            for (var indexName : concreteIndices) {
                IndexMetadata indexMetadata = currentState.metadata().index(indexName);
                assert indexMetadata != null : "Cannot resolve indexMetadata for relation=" + relationName;
                var settingsBuilder = Settings.builder().put(indexMetadata.getSettings());
                settingsBuilder.remove(REPLICATION_SUBSCRIPTION_NAME.getKey());
                mdBuilder.put(
                    IndexMetadata
                        .builder(indexMetadata)
                        .settingsVersion(1 + indexMetadata.getSettingsVersion())
                        .settings(settingsBuilder)
                );
            }

            var possibleTemplateName = PartitionName.templateName(relationName.schema(), relationName.name());
            var templateMetadata = currentState.metadata().templates().get(possibleTemplateName);
            if (templateMetadata != null) {
                var templateBuilder = new IndexTemplateMetadata.Builder(templateMetadata);
                var settingsBuilder = Settings.builder()
                    .put(templateMetadata.settings());
                settingsBuilder.remove(REPLICATION_SUBSCRIPTION_NAME.getKey());
                mdBuilder.put(templateBuilder.settings(settingsBuilder.build()).build());
            }
        }
    }
}
