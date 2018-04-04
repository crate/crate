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
import io.crate.metadata.UsersPrivilegesMetaData;
import io.crate.metadata.cluster.DDLClusterStateModifier;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;

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
        MetaData currentMetaData = currentState.metaData();
        MetaData.Builder mdBuilder = MetaData.builder(currentMetaData);
        if (transferTablePrivileges(mdBuilder, sourceRelationName, targetRelationName)) {
            return ClusterState.builder(currentState).metaData(mdBuilder).build();
        }
        return currentState;
    }

    private ClusterState dropPrivilegesForTableOrView(ClusterState currentState, RelationName relationName) {
        MetaData currentMetaData = currentState.metaData();
        MetaData.Builder mdBuilder = MetaData.builder(currentMetaData);

        if (dropPrivileges(mdBuilder, relationName) == false) {
            // if nothing is affected, don't modify the state and just return the given currentState
            return currentState;
        }

        return ClusterState.builder(currentState).metaData(mdBuilder).build();
    }

    private static boolean dropPrivileges(MetaData.Builder mdBuilder, RelationName relationName) {
        // create a new instance of the metadata, to guarantee the cluster changed action.
        UsersPrivilegesMetaData newMetaData = UsersPrivilegesMetaData.copyOf(
            (UsersPrivilegesMetaData) mdBuilder.getCustom(UsersPrivilegesMetaData.TYPE));

        long affectedRows = newMetaData.dropTableOrViewPrivileges(relationName.fqn());
        mdBuilder.putCustom(UsersPrivilegesMetaData.TYPE, newMetaData);
        return affectedRows > 0L;
    }

    private static boolean transferTablePrivileges(MetaData.Builder mdBuilder,
                                                RelationName sourceRelationName,
                                                RelationName targetRelationName) {
        UsersPrivilegesMetaData oldMetaData = (UsersPrivilegesMetaData) mdBuilder.getCustom(UsersPrivilegesMetaData.TYPE);
        if (oldMetaData == null) {
            return false;
        }

        // create a new instance of the metadata if privileges were changed, to guarantee the cluster changed action.
        UsersPrivilegesMetaData newMetaData = UsersPrivilegesMetaData.maybeCopyAndReplaceTableIdents(
            oldMetaData, sourceRelationName.fqn(), targetRelationName.fqn());

        if (newMetaData != null) {
            mdBuilder.putCustom(UsersPrivilegesMetaData.TYPE, newMetaData);
            return true;
        }
        return false;
    }
}
