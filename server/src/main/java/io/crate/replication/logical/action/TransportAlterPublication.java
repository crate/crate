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
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.cluster.snapshots.restore.TableOrPartition;
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
import io.crate.common.annotations.VisibleForTesting;

import io.crate.exceptions.RelationUnknown;
import io.crate.metadata.RelationName;
import io.crate.replication.logical.exceptions.PublicationUnknownException;
import io.crate.replication.logical.metadata.Publication;
import io.crate.replication.logical.metadata.PublicationsMetadata;
import io.crate.sql.tree.AlterPublication;

public class TransportAlterPublication extends TransportMasterNodeAction<TransportAlterPublication.Request, AcknowledgedResponse> {

    public static final Action ACTION = new Action();
    private static final Logger LOGGER = LogManager.getLogger(TransportAlterPublication.class);

    public static class Action extends ActionType<AcknowledgedResponse> {
        private static final String NAME = "internal:crate:replication/logical/publication/alter";

        private Action() {
            super(NAME);
        }
    }

    @Inject
    public TransportAlterPublication(TransportService transportService,
                                     ClusterService clusterService,
                                     ThreadPool threadPool) {
        super(
            ACTION.name(),
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
        for (var target : request.targets) {
            var relation = target.table();
            if (currentMetadata.getRelation(relation) == null) {
                throw new RelationUnknown(relation);
            }
        }

        HashSet<TableOrPartition> targets = new HashSet<>();
        switch (request.operation) {
            case SET -> targets = new HashSet<>(request.targets);
            case ADD -> {
                targets.addAll(oldPublication.targets());
                targets.addAll(request.targets);
            }
            case DROP -> oldPublication.targets().stream()
                .filter(target -> request.targets.contains(target) == false)
                .forEach(targets::add);
            default ->
                throw new UnsupportedOperationException(
                    "Alter publication operation '" + request.operation + "' is not supported"
                );
        }
        return new Publication(oldPublication.owner(), oldPublication.isForAllTables(), new ArrayList<>(targets));
    }

    public static class Request extends AcknowledgedRequest<Request> {

        private final String name;
        private final AlterPublication.Operation operation;
        private final List<TableOrPartition> targets;

        public Request(String name, AlterPublication.Operation operation, List<TableOrPartition> targets) {
            this.name = name;
            this.operation = operation;
            this.targets = targets;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            name = in.readString();
            operation = AlterPublication.Operation.VALUES[in.readVInt()];
            if (in.getVersion().before(Version.V_6_5_0)) {
                targets = in.readList(stream -> new TableOrPartition(new RelationName(stream), null));
            } else {
                targets = in.readList(TableOrPartition::new);
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(name);
            out.writeVInt(operation.ordinal());
            if (out.getVersion().before(Version.V_6_5_0)) {
                for (var target : targets) {
                    if (target.partitionIdent() != null) {
                        throw new IllegalStateException("Cannot write partition publication target to a node before " + Version.V_6_5_0);
                    }
                }
                out.writeCollection(targets, (stream, target) -> {
                    target.table().writeTo(stream);
                });
            } else {
                out.writeList(targets);
            }
        }
    }
}
