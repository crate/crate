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
import io.crate.replication.logical.exceptions.PublicationUnknownException;
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
public class TransportDropPublicationAction extends AbstractDDLTransportAction<DropPublicationRequest, AcknowledgedResponse> {

    public static final String ACTION_NAME = "internal:crate:replication/logical/publication/drop";


    @Inject
    public TransportDropPublicationAction(TransportService transportService,
                                          ClusterService clusterService,
                                          ThreadPool threadPool,
                                          IndexNameExpressionResolver indexNameExpressionResolver) {
        super(ACTION_NAME,
              transportService,
              clusterService,
              threadPool,
              indexNameExpressionResolver,
              DropPublicationRequest::new,
              AcknowledgedResponse::new,
              AcknowledgedResponse::new,
              "drop-publication");
    }

    @Override
    public ClusterStateTaskExecutor<DropPublicationRequest> clusterStateTaskExecutor(DropPublicationRequest request) {
        return new DDLClusterStateTaskExecutor<>() {
            @Override
            protected ClusterState execute(ClusterState currentState, DropPublicationRequest request) throws Exception {
                Metadata currentMetadata = currentState.metadata();
                Metadata.Builder mdBuilder = Metadata.builder(currentMetadata);

                PublicationsMetadata oldMetadata = (PublicationsMetadata) mdBuilder.getCustom(PublicationsMetadata.TYPE);
                if (oldMetadata == null && request.ifExists() == false) {
                    throw new PublicationUnknownException(request.name());
                } else if (oldMetadata != null) {
                    if (oldMetadata.publications().containsKey(request.name())) {

                        PublicationsMetadata newMetadata = PublicationsMetadata.newInstance(oldMetadata);
                        newMetadata.publications().remove(request.name());
                        assert !newMetadata.equals(oldMetadata) : "must not be equal to guarantee the cluster change action";
                        mdBuilder.putCustom(PublicationsMetadata.TYPE, newMetadata);

                        return ClusterState.builder(currentState).metadata(mdBuilder).build();
                    } else if (request.ifExists() == false) {
                        throw new PublicationUnknownException(request.name());
                    }
                }

                return currentState;
            }
        };
    }

    @Override
    protected ClusterBlockException checkBlock(DropPublicationRequest request,
                                               ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
