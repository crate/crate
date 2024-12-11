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
import org.elasticsearch.common.settings.Settings;
import org.jetbrains.annotations.VisibleForTesting;

public final class AlterServerTask extends AckedClusterStateUpdateTask<AcknowledgedResponse> {

    private final AlterServerRequest request;

    public AlterServerTask(AlterServerRequest request) {
        super(Priority.NORMAL, request);
        this.request = request;
    }

    @Override
    protected AcknowledgedResponse newResponse(boolean acknowledged) {
        return new AcknowledgedResponse(acknowledged);
    }

    @Override
    public ClusterState execute(ClusterState currentState) throws Exception {
        ServersMetadata serversMetadata = currentState.metadata().custom(
            ServersMetadata.TYPE,
            ServersMetadata.EMPTY
        );
        var serverName = request.name();
        ServersMetadata.Server oldServer = serversMetadata.get(serverName);
        var optionsBuilder = Settings.builder().put(oldServer.options());

        processOptions(request, optionsBuilder);

        ServersMetadata.Server newServer = serversMetadata.get(serverName)
            .withOptions(optionsBuilder.build());

        var newServersMetadata = serversMetadata.put(serverName, newServer);
        return ClusterState.builder(currentState)
            .metadata(
                Metadata.builder(currentState.metadata())
                    .putCustom(ServersMetadata.TYPE, newServersMetadata)
            )
            .build();
    }

    @VisibleForTesting
    static void processOptions(AlterServerRequest request, Settings.Builder optionsBuilder) {
        var serverName = request.name();
        for (var entry : request.optionsAdded().getAsStructuredMap().entrySet()) {
            if (optionsBuilder.get(entry.getKey()) != null) {
                throw new IllegalArgumentException(String.format(
                    Locale.ENGLISH,
                    "Option `%s` already exists for server `%s`, use SET to change it",
                    entry.getKey(),
                    serverName
                ));
            }
            optionsBuilder.put(entry.getKey(), entry.getValue());
        }
        for (var entry : request.optionsUpdated().getAsStructuredMap().entrySet()) {
            if (optionsBuilder.get(entry.getKey()) == null) {
                throw new IllegalArgumentException(String.format(
                    Locale.ENGLISH,
                    "Option `%s` does not exist for server `%s`, use ADD to add it",
                    entry.getKey(),
                    serverName
                ));
            }
            optionsBuilder.put(entry.getKey(), entry.getValue());
        }
        for (var option : request.optionsRemoved()) {
            if (optionsBuilder.get(option) == null) {
                throw new IllegalArgumentException(String.format(
                    Locale.ENGLISH,
                    "Option `%s` does not exist for server `%s`",
                    option,
                    serverName
                ));
            }
            optionsBuilder.remove(option);
        }
    }
}
