/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.fdw;

import java.util.Locale;

import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.Priority;

import io.crate.exceptions.RelationAlreadyExists;
import io.crate.metadata.RelationName;

final class AddForeignTableTask extends AckedClusterStateUpdateTask<AcknowledgedResponse> {

    private final CreateForeignTableRequest request;

    AddForeignTableTask(CreateForeignTableRequest request) {
        super(Priority.NORMAL, request);
        this.request = request;
    }

    @Override
    protected AcknowledgedResponse newResponse(boolean acknowledged) {
        return new AcknowledgedResponse(acknowledged);
    }

    @Override
    public ClusterState execute(ClusterState currentState) throws Exception {
        RelationName tableName = request.tableName();
        Metadata metadata = currentState.metadata();
        if (metadata.contains(tableName)) {
            if (request.ifNotExists()) {
                return currentState;
            }
            throw new RelationAlreadyExists(tableName);
        }
        ServersMetadata serversMetadata = metadata.custom(ServersMetadata.TYPE);
        if (serversMetadata == null) {
            serversMetadata = ServersMetadata.EMPTY;
        }
        if (!serversMetadata.contains(request.server())) {
            throw new IllegalArgumentException(String.format(
                Locale.ENGLISH,
                "Cannot create foreign table for server `%s`. It doesn't exist. Create it using CREATE SERVER",
                request.server()
                ));
        }


        ForeignTablesMetadata foreignTables = metadata.custom(ForeignTablesMetadata.TYPE);
        if (foreignTables == null) {
            foreignTables = ForeignTablesMetadata.EMPTY;
        }

        ForeignTablesMetadata updatedTables = foreignTables.add(
            tableName,
            request.columns(),
            request.server(),
            request.options()
        );

        return ClusterState.builder(currentState)
            .metadata(Metadata.builder(metadata)
                .putCustom(ForeignTablesMetadata.TYPE, updatedTables)
            )
            .build();
    }
}
