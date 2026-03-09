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

import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RelationMetadata;
import org.elasticsearch.common.Priority;

import io.crate.exceptions.RelationUnknown;
import io.crate.metadata.RelationName;
import io.crate.sql.tree.CascadeMode;

public class DropForeignTableTask extends AckedClusterStateUpdateTask<AcknowledgedResponse> {

    private final DropForeignTableRequest request;

    DropForeignTableTask(DropForeignTableRequest request) {
        super(Priority.NORMAL, request);
        this.request = request;
    }

    @Override
    protected AcknowledgedResponse newResponse(boolean acknowledged) {
        return new AcknowledgedResponse(acknowledged);
    }

    @Override
    public ClusterState execute(ClusterState currentState) throws Exception {
        if (request.cascadeMode() == CascadeMode.CASCADE) {
            throw new UnsupportedOperationException("DROP FOREIGN TABLE with CASCADE is not supported");
        }

        Metadata metadata = currentState.metadata();
        Metadata.Builder mdBuilder = Metadata.builder(metadata);
        boolean dropped = false;

        for (RelationName name : request.names()) {
            RelationMetadata relationMetadata = metadata.getRelation(name);
            if (relationMetadata instanceof RelationMetadata.ForeignTable) {
                mdBuilder.dropRelation(name);
                dropped = true;
            } else if (request.ifExists() == false) {
                throw new RelationUnknown(name);
            }
        }

        if (dropped == false) {
            return currentState;
        }
        return ClusterState.builder(currentState)
            .metadata(mdBuilder)
            .build();
    }
}
