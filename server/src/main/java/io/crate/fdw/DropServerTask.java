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

import io.crate.sql.tree.CascadeMode;

public class DropServerTask extends AckedClusterStateUpdateTask<AcknowledgedResponse> {

    private final DropServerRequest request;

    public DropServerTask(DropServerRequest request) {
        super(Priority.NORMAL, request);
        this.request = request;
    }

    @Override
    protected AcknowledgedResponse newResponse(boolean acknowledged) {
        return new AcknowledgedResponse(acknowledged);
    }

    @Override
    public ClusterState execute(ClusterState currentState) throws Exception {
        Metadata metadata = currentState.metadata();
        ServersMetadata servers = metadata.custom(ServersMetadata.TYPE, ServersMetadata.EMPTY);
        ForeignTablesMetadata foreignTables = metadata.custom(ForeignTablesMetadata.TYPE, ForeignTablesMetadata.EMPTY);
        ForeignTablesMetadata updatedForeignTables = foreignTables;
        if (request.mode() == CascadeMode.RESTRICT) {
            for (String serverName : request.names()) {
                if (foreignTables.anyDependOnServer(serverName)) {
                    throw new IllegalArgumentException(String.format(
                        Locale.ENGLISH,
                        "Cannot drop server `%s` because foreign tables depend on it",
                        serverName
                    ));
                }
            }
        } else {
            updatedForeignTables = foreignTables.removeAllForServers(request.names());
        }

        ServersMetadata updatedServers = servers.remove(
            request.names(),
            request.ifExists(),
            request.mode()
        );
        if (updatedServers == servers && updatedForeignTables == foreignTables) {
            return currentState;
        }
        return ClusterState.builder(currentState)
            .metadata(
                Metadata.builder(metadata)
                    .putCustom(ServersMetadata.TYPE, updatedServers)
                    .putCustom(ForeignTablesMetadata.TYPE, updatedForeignTables)
            )
            .build();
    }
}

