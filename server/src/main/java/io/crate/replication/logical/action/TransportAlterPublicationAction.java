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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import org.jetbrains.annotations.VisibleForTesting;
import io.crate.exceptions.RelationUnknown;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import io.crate.replication.logical.exceptions.PublicationUnknownException;
import io.crate.replication.logical.metadata.Publication;
import io.crate.replication.logical.metadata.PublicationsMetadata;
import io.crate.sql.tree.AlterPublication;

public class TransportAlterPublicationAction extends TransportMasterNodeAction<TransportAlterPublicationAction.Request, AcknowledgedResponse> {

    public static final String NAME = "internal:crate:replication/logical/publication/alter";

    private static final Logger LOGGER = LogManager.getLogger(TransportAlterPublicationAction.class);

    @Inject
    public TransportAlterPublicationAction(TransportService transportService,
                                           ClusterService clusterService,
                                           ThreadPool threadPool) {
        super(
            NAME,
            transportService,
            clusterService,
            threadPool,
            Request::new
        );
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
    protected ClusterBlockException checkBlock(Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected void masterOperation(Request request,
                                   ClusterState state,
                                   ActionListener<AcknowledgedResponse> listener) throws Exception {
        var updateTask = new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                Metadata currentMetadata = currentState.metadata();
                Metadata.Builder mdBuilder = Metadata.builder(currentMetadata);

                PublicationsMetadata oldMetadata = (PublicationsMetadata) mdBuilder.getCustom(PublicationsMetadata.TYPE);
                if (oldMetadata == null) {
                    throw new PublicationUnknownException(request.name);
                } else {
                    var publication = oldMetadata.publications().get(request.name);
                    if (publication != null) {
                        var newPublication = updatePublication(request, currentMetadata, publication);

                        PublicationsMetadata newMetadata = PublicationsMetadata.newInstance(oldMetadata);
                        newMetadata.publications().put(request.name, newPublication);
                        assert !newMetadata.equals(oldMetadata) : "must not be equal to guarantee the cluster change action";
                        mdBuilder.putCustom(PublicationsMetadata.TYPE, newMetadata);

                        return ClusterState.builder(currentState).metadata(mdBuilder).build();
                    } else {
                        throw new PublicationUnknownException(request.name);
                    }
                }
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                listener.onResponse(new AcknowledgedResponse(true));
            }

            @Override
            public void onFailure(String source, Exception e) {
                if (LOGGER.isTraceEnabled()) {
                    logger.trace("Error while trying to alter publication " + request.name, e);
                }
                listener.onFailure(e);
            }
        };

        clusterService.submitStateUpdateTask("alter-publication", updateTask);
    }

    @VisibleForTesting
    static Publication updatePublication(Request request, Metadata currentMetadata, Publication oldPublication) {
        // Ensure tables exists
        for (var relation : request.tables) {
            if (currentMetadata.hasIndex(relation.indexNameOrAlias()) == false
                && currentMetadata.templates().containsKey(PartitionName.templateName(relation.schema(), relation.name())) == false) {
                throw new RelationUnknown(relation);
            }
        }

        HashSet<RelationName> tables = new HashSet<>();
        switch (request.operation) {
            case SET -> tables = new HashSet<>(request.tables);
            case ADD -> {
                tables.addAll(oldPublication.tables());
                tables.addAll(request.tables);
            }
            case DROP -> oldPublication.tables().stream()
                .filter(relationName -> request.tables.contains(relationName) == false)
                .forEach(tables::add);
            default ->
                throw new UnsupportedOperationException(
                    "Alter publication operation '" + request.operation + "' is not supported"
                );
        }
        return new Publication(oldPublication.owner(), oldPublication.isForAllTables(), new ArrayList<>(tables));
    }

    public static class Request extends AcknowledgedRequest<Request> {

        private final String name;
        private final AlterPublication.Operation operation;
        private final List<RelationName> tables;

        public Request(String name, AlterPublication.Operation operation, List<RelationName> tables) {
            this.name = name;
            this.operation = operation;
            this.tables = tables;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            name = in.readString();
            operation = AlterPublication.Operation.VALUES[in.readVInt()];
            tables = in.readList(RelationName::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(name);
            out.writeVInt(operation.ordinal());
            out.writeList(tables);
        }
    }
}
