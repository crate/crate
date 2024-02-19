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

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.Priority;

public class AddUserMappingTask extends AckedClusterStateUpdateTask<AcknowledgedResponse> {

    private final CreateUserMappingRequest request;

    AddUserMappingTask(CreateUserMappingRequest request) {
        super(Priority.NORMAL, request);
        this.request = request;
    }

    @Override
    protected AcknowledgedResponse newResponse(boolean acknowledged) {
        return new AcknowledgedResponse(acknowledged);
    }

    @Override
    public ClusterState execute(ClusterState currentState) throws Exception {
        ServersMetadata serversMetadata = currentState.metadata().custom(ServersMetadata.TYPE);
        if (serversMetadata == null) {
            throw new ResourceNotFoundException(String.format(
                Locale.ENGLISH,
                "Server `%s` not found",
                request.server()
            ));
        }
        var updatedServersMetadata = serversMetadata.addUser(
            request.server(),
            request.ifNotExists(),
            request.userName(),
            request.options()
        );
        if (updatedServersMetadata == serversMetadata) {
            return currentState;
        }
        return ClusterState.builder(currentState)
            .metadata(
                Metadata.builder(currentState.metadata())
                    .putCustom(ServersMetadata.TYPE, updatedServersMetadata)
            )
            .build();
    }
}
