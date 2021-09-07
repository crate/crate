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

import io.crate.exceptions.RelationUnknown;
import io.crate.execution.ddl.AbstractDDLTransportAction;
import io.crate.metadata.PartitionName;
import io.crate.metadata.cluster.DDLClusterStateTaskExecutor;
import io.crate.replication.logical.exceptions.PublicationAlreadyExistsException;
import io.crate.replication.logical.metadata.Publication;
import io.crate.replication.logical.metadata.PublicationsMetadata;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

@Singleton
public class TransportCreatePublicationAction extends AbstractDDLTransportAction<CreatePublicationRequest, AcknowledgedResponse> {

    public static final String ACTION_NAME = "internal:crate:replication/logical/publication/create";

    @Inject
    public TransportCreatePublicationAction(TransportService transportService,
                                            ClusterService clusterService,
                                            ThreadPool threadPool,
                                            IndexNameExpressionResolver indexNameExpressionResolver) {
        super(ACTION_NAME,
              transportService,
              clusterService,
              threadPool,
              indexNameExpressionResolver,
              CreatePublicationRequest::new,
              AcknowledgedResponse::new,
              AcknowledgedResponse::new,
              "create-publication");
    }

    @Override
    public ClusterStateTaskExecutor<CreatePublicationRequest> clusterStateTaskExecutor(CreatePublicationRequest request) {
        return new DDLClusterStateTaskExecutor<>() {
            @Override
            protected ClusterState execute(ClusterState currentState,
                                           CreatePublicationRequest request) throws Exception {
                Metadata currentMetadata = currentState.metadata();
                Metadata.Builder mdBuilder = Metadata.builder(currentMetadata);

                var oldMetadata = (PublicationsMetadata) mdBuilder.getCustom(PublicationsMetadata.TYPE);
                if (oldMetadata != null && oldMetadata.publications().containsKey(request.name())) {
                    throw new PublicationAlreadyExistsException(request.name());
                }

                // Ensure tables exists
                for (var relation : request.tables()) {
                    if (currentMetadata.hasIndex(relation.indexNameOrAlias()) == false
                        && currentMetadata.templates().containsKey(PartitionName.templateName(relation.schema(), relation.name())) == false) {
                        throw new RelationUnknown(relation);
                    }
                }

                // create a new instance of the metadata, to guarantee the cluster changed action.
                var newMetadata = PublicationsMetadata.newInstance(oldMetadata);
                newMetadata.publications().put(
                    request.name(),
                    new Publication(request.owner(), request.isForAllTables(), request.tables())
                );
                assert !newMetadata.equals(oldMetadata) : "must not be equal to guarantee the cluster change action";
                mdBuilder.putCustom(PublicationsMetadata.TYPE, newMetadata);

                return ClusterState.builder(currentState).metadata(mdBuilder).build();
            }
        };
    }

    @Override
    protected ClusterBlockException checkBlock(CreatePublicationRequest request,
                                               ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
