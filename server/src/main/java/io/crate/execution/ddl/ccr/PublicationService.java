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

package io.crate.execution.ddl.ccr;

import io.crate.replication.logical.metadata.PublicationsMetadata;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;

import java.util.concurrent.CompletableFuture;

public class PublicationService implements ClusterStateListener {

    private static final Logger LOGGER = LogManager.getLogger(PublicationService.class);

    public final ClusterService clusterService;
    public final TransportCreatePublicationAction createPublicationAction;
    private PublicationsMetadata publications;

    @Inject
    public PublicationService(ClusterService clusterService,
                              TransportCreatePublicationAction createPublicationAction) {
        this.clusterService = clusterService;
        this.createPublicationAction = createPublicationAction;
        clusterService.addListener(this);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        var prevMetadata = event.previousState().metadata();
        var newMetadata = event.state().metadata();

        PublicationsMetadata prevPublicationsMetadata = prevMetadata.custom(PublicationsMetadata.TYPE);
        PublicationsMetadata newPublicationsMetadata = newMetadata.custom(PublicationsMetadata.TYPE);

        if (prevPublicationsMetadata != newPublicationsMetadata) {
            publications = newPublicationsMetadata;
        }
    }

    public CompletableFuture<Long> createPublication(CreatePublicationRequest request) {
        final CompletableFuture<Long> future = new CompletableFuture<>();
        createPublicationAction.execute(
            request,
            new ActionListener<>() {
                @Override
                public void onResponse(AcknowledgedResponse response) {
                    if (!response.isAcknowledged()) {
                        LOGGER.info("create publication '{}' not acknowledged", request.name());
                    }
                    future.complete(1L);
                }

                @Override
                public void onFailure(Exception e) {
                    future.completeExceptionally(e);
                }
            }
        );
        return future;
    }
}
