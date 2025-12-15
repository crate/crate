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

package io.crate.role;

import java.util.List;
import java.util.Objects;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;

import io.crate.common.annotations.VisibleForTesting;
import io.crate.metadata.RelationName;
import io.crate.metadata.cluster.DDLClusterStateModifier;
import io.crate.role.metadata.RolesMetadata;
import io.crate.role.metadata.UsersMetadata;
import io.crate.role.metadata.UsersPrivilegesMetadata;

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
        if (transferTablePrivileges(
            currentState.nodes().getMinNodeVersion(),
            mdBuilder,
            sourceRelationName,
            targetRelationName)) {
            return ClusterState.builder(currentState).metadata(mdBuilder).build();
        }
        return currentState;
    }

    @Override
    public ClusterState onSwapRelations(ClusterState currentState, RelationName source, RelationName target) {
        Metadata currentMetadata = currentState.metadata();
        Metadata.Builder mdBuilder = Metadata.builder(currentMetadata);

        UsersPrivilegesMetadata oldPrivilegesMetadata = (UsersPrivilegesMetadata) mdBuilder.getCustom(UsersPrivilegesMetadata.TYPE);
        RolesMetadata oldRolesMetadata = (RolesMetadata) mdBuilder.getCustom(RolesMetadata.TYPE);
        if (oldPrivilegesMetadata == null && oldRolesMetadata == null) {
            return currentState;
        }
        validateMigrationToRolesMetadata(currentState.nodes().getMinNodeVersion(), "swap tables");

        var oldUsersMetadata = (UsersMetadata) mdBuilder.getCustom(UsersMetadata.TYPE);
        RolesMetadata newMetadata = RolesMetadata.of(mdBuilder, oldUsersMetadata, oldPrivilegesMetadata, oldRolesMetadata);

        newMetadata = PrivilegesModifier.swapPrivileges(newMetadata, source, target);
        return ClusterState.builder(currentState)
            .metadata(Metadata.builder(currentMetadata)
                .putCustom(RolesMetadata.TYPE, newMetadata)
                .build())
            .build();
    }

    private ClusterState dropPrivilegesForTableOrView(ClusterState currentState, RelationName relationName) {
        Metadata currentMetadata = currentState.metadata();
        Metadata.Builder mdBuilder = Metadata.builder(currentMetadata);

        if (dropPrivileges(currentState.nodes().getMinNodeVersion(), mdBuilder, relationName) == false) {
            // if nothing is affected, don't modify the state and just return the given currentState
            return currentState;
        }

        return ClusterState.builder(currentState).metadata(mdBuilder).build();
    }

    private static boolean dropPrivileges(Version minNodeVersion, Metadata.Builder mdBuilder, RelationName relationName) {
        UsersPrivilegesMetadata oldPrivilegesMetadata = (UsersPrivilegesMetadata) mdBuilder.getCustom(UsersPrivilegesMetadata.TYPE);
        RolesMetadata oldRolesMetadata = (RolesMetadata) mdBuilder.getCustom(RolesMetadata.TYPE);
        if (oldPrivilegesMetadata == null && oldRolesMetadata == null) {
            return false;
        }
        validateMigrationToRolesMetadata(minNodeVersion, "drop tables or views");

        var oldUsersMetadata = (UsersMetadata) mdBuilder.getCustom(UsersMetadata.TYPE);
        RolesMetadata newMetadata = RolesMetadata.of(mdBuilder, oldUsersMetadata, oldPrivilegesMetadata, oldRolesMetadata);

        long affectedRows = PrivilegesModifier.dropTableOrViewPrivileges(mdBuilder, newMetadata, relationName.fqn());
        return affectedRows > 0L;
    }

    @VisibleForTesting
    static boolean transferTablePrivileges(Version minNodeVersion,
                                           Metadata.Builder mdBuilder,
                                           RelationName sourceRelationName,
                                           RelationName targetRelationName) {
        UsersPrivilegesMetadata oldPrivilegesMetadata = (UsersPrivilegesMetadata) mdBuilder.getCustom(UsersPrivilegesMetadata.TYPE);
        RolesMetadata oldRolesMetadata = (RolesMetadata) mdBuilder.getCustom(RolesMetadata.TYPE);
        if (oldPrivilegesMetadata == null && oldRolesMetadata == null) {
            return false;
        }
        validateMigrationToRolesMetadata(minNodeVersion, "rename tables or views");

        var oldUsersMetadata = (UsersMetadata) mdBuilder.getCustom(UsersMetadata.TYPE);
        RolesMetadata migratedMetadata = RolesMetadata.of(mdBuilder, oldUsersMetadata, oldPrivilegesMetadata, oldRolesMetadata);

        // create a new instance of the metadata if privileges were changed, to guarantee the cluster changed action.
        RolesMetadata newMetadata = PrivilegesModifier.maybeCopyAndReplaceTableIdents(migratedMetadata, sourceRelationName.fqn(), targetRelationName.fqn());

        if (newMetadata != null) {
            mdBuilder.putCustom(RolesMetadata.TYPE, newMetadata);
            return true;
        } else if (Objects.equals(oldRolesMetadata, migratedMetadata) == false) {
            mdBuilder.putCustom(RolesMetadata.TYPE, migratedMetadata);
            return true;
        }
        return false;
    }

    private static void validateMigrationToRolesMetadata(Version minNodeVersion, String msg) {
        if (minNodeVersion.onOrAfter(Version.V_5_6_0) == false) {
            throw new IllegalStateException("Cannot " + msg + " until all nodes are upgraded to 5.6");
        }
    }
}
