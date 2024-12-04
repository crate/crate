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

import static io.crate.replication.logical.LogicalReplicationSettings.REPLICATION_SUBSCRIPTION_NAME;

import java.util.Collection;

import org.elasticsearch.action.ActionType;
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
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import io.crate.execution.ddl.AbstractDDLTransportAction;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import io.crate.metadata.cluster.DDLClusterStateTaskExecutor;
import io.crate.replication.logical.exceptions.SubscriptionUnknownException;
import io.crate.replication.logical.metadata.SubscriptionsMetadata;

public class DropSubscriptionAction extends ActionType<AcknowledgedResponse> {

    public static final String NAME = "internal:crate:replication/logical/subscription/drop";
    public static final DropSubscriptionAction INSTANCE = new DropSubscriptionAction();

    public DropSubscriptionAction() {
        super(NAME);
    }

    @Override
    public Writeable.Reader<AcknowledgedResponse> getResponseReader() {
        return AcknowledgedResponse::new;
    }

    /**
     * Removes the REPLICATION_SUBSCRIPTION_NAME index setting from all indices.
     * (Without this setting, indices will use the default read-write engine)
     */
    public static ClusterState removeSubscriptionSetting(Collection<RelationName> relations,
                                                  ClusterState currentState,
                                                  Metadata.Builder mdBuilder) {
        Metadata metadata = currentState.metadata();
        for (var relationName : relations) {
            var concreteIndices = IndexNameExpressionResolver.concreteIndexNames(
                metadata,
                IndicesOptions.LENIENT_EXPAND_OPEN,
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
        return ClusterState.builder(currentState).metadata(mdBuilder).build();
    }

    @Singleton
    public static class TransportAction extends AbstractDDLTransportAction<DropSubscriptionRequest, AcknowledgedResponse> {

        @Inject
        public TransportAction(TransportService transportService,
                               ClusterService clusterService,
                               ThreadPool threadPool) {
            super(NAME,
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

                        SubscriptionsMetadata newMetadata = SubscriptionsMetadata.newInstance(oldMetadata);
                        var subscription = newMetadata.subscription().remove(request.name());
                        assert !newMetadata.equals(oldMetadata) : "must not be equal to guarantee the cluster change action";
                        mdBuilder.putCustom(SubscriptionsMetadata.TYPE, newMetadata);

                        return removeSubscriptionSetting(subscription.relations().keySet(), currentState, mdBuilder);
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
    }
}
