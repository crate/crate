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

package io.crate.user;

import io.crate.metadata.RelationName;
import io.crate.user.metadata.UsersPrivilegesMetadata;
import io.crate.metadata.cluster.DDLClusterStateModifier;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;

import java.util.List;

public class RoleManagerDDLModifier implements DDLClusterStateModifier {

    @Override
    public ClusterState onDropTable(ClusterState currentState, RelationName relationName) {
        return dropPrivilegesForTableOrView(currentState, relationName);
    }

    @Override
    public ClusterState onDropView(ClusterState currentState, List<RelationName> relationNames) {
        for (RelationName relationName : relationNames) {
            currentState = dropPrivilegesForTableOrView(currentState, relationName);
        }
        return currentState;
    }

    @Override
    public ClusterState onRenameTable(ClusterState currentState,
                                      RelationName sourceRelationName,
                                      RelationName targetRelationName,
                                      boolean isPartitionedTable) {
        Metadata currentMetadata = currentState.metadata();
        Metadata.Builder mdBuilder = Metadata.builder(currentMetadata);
        if (transferTablePrivileges(mdBuilder, sourceRelationName, targetRelationName)) {
            return ClusterState.builder(currentState).metadata(mdBuilder).build();
        }
        return currentState;
    }

    @Override
    public ClusterState onSwapRelations(ClusterState currentState, RelationName source, RelationName target) {
        Metadata currentMetadata = currentState.metadata();
        UsersPrivilegesMetadata userPrivileges = currentMetadata.custom(UsersPrivilegesMetadata.TYPE);
        if (userPrivileges == null) {
            return currentState;
        }
        UsersPrivilegesMetadata updatedPrivileges = UsersPrivilegesMetadata.swapPrivileges(userPrivileges, source, target);
        return ClusterState.builder(currentState)
            .metadata(Metadata.builder(currentMetadata)
                .putCustom(UsersPrivilegesMetadata.TYPE, updatedPrivileges)
                .build())
            .build();
    }

    private ClusterState dropPrivilegesForTableOrView(ClusterState currentState, RelationName relationName) {
        Metadata currentMetadata = currentState.metadata();
        Metadata.Builder mdBuilder = Metadata.builder(currentMetadata);

        if (dropPrivileges(mdBuilder, relationName) == false) {
            // if nothing is affected, don't modify the state and just return the given currentState
            return currentState;
        }

        return ClusterState.builder(currentState).metadata(mdBuilder).build();
    }

    private static boolean dropPrivileges(Metadata.Builder mdBuilder, RelationName relationName) {
        // create a new instance of the metadata, to guarantee the cluster changed action.
        UsersPrivilegesMetadata newMetadata = UsersPrivilegesMetadata.copyOf(
            (UsersPrivilegesMetadata) mdBuilder.getCustom(UsersPrivilegesMetadata.TYPE));

        long affectedRows = newMetadata.dropTableOrViewPrivileges(relationName.fqn());
        mdBuilder.putCustom(UsersPrivilegesMetadata.TYPE, newMetadata);
        return affectedRows > 0L;
    }

    private static boolean transferTablePrivileges(Metadata.Builder mdBuilder,
                                                   RelationName sourceRelationName,
                                                   RelationName targetRelationName) {
        UsersPrivilegesMetadata oldMetadata = (UsersPrivilegesMetadata) mdBuilder.getCustom(UsersPrivilegesMetadata.TYPE);
        if (oldMetadata == null) {
            return false;
        }

        // create a new instance of the metadata if privileges were changed, to guarantee the cluster changed action.
        UsersPrivilegesMetadata newMetadata = UsersPrivilegesMetadata.maybeCopyAndReplaceTableIdents(
            oldMetadata, sourceRelationName.fqn(), targetRelationName.fqn());

        if (newMetadata != null) {
            mdBuilder.putCustom(UsersPrivilegesMetadata.TYPE, newMetadata);
            return true;
        }
        return false;
    }
}
