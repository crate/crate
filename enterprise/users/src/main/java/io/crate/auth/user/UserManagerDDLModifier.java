/*
 * This file is part of a module with proprietary Enterprise Features.
 *
 * Licensed to Crate.io Inc. ("Crate.io") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 *
 * To use this file, Crate.io must have given you permission to enable and
 * use such Enterprise Features and you must have a valid Enterprise or
 * Subscription Agreement with Crate.io.  If you enable or use the Enterprise
 * Features, you represent and warrant that you have a valid Enterprise or
 * Subscription Agreement with Crate.io.  Your use of the Enterprise Features
 * if governed by the terms and conditions of your Enterprise or Subscription
 * Agreement with Crate.io.
 */

package io.crate.auth.user;

import io.crate.metadata.RelationName;
import io.crate.metadata.UsersPrivilegesMetadata;
import io.crate.metadata.cluster.DDLClusterStateModifier;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;

import java.util.List;

public class UserManagerDDLModifier implements DDLClusterStateModifier {

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
