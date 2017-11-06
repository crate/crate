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

package io.crate.operation.user;

import com.google.common.annotations.VisibleForTesting;
import io.crate.metadata.UsersMetaData;
import io.crate.metadata.UsersPrivilegesMetaData;
import io.crate.user.SecureHash;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Collections;

public class TransportCreateUserAction extends TransportMasterNodeAction<CreateUserRequest, WriteUserResponse> {

    @Inject
    public TransportCreateUserAction(Settings settings,
                              TransportService transportService,
                              ClusterService clusterService,
                              ThreadPool threadPool,
                              ActionFilters actionFilters,
                              IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, "crate/sql/create_user", transportService, clusterService, threadPool, actionFilters,
            indexNameExpressionResolver, CreateUserRequest::new);
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.GENERIC;
    }

    @Override
    protected WriteUserResponse newResponse() {
        return new WriteUserResponse(false);
    }

    @Override
    protected void masterOperation(CreateUserRequest request, ClusterState state, ActionListener<WriteUserResponse> listener) throws Exception {
        clusterService.submitStateUpdateTask("create_user [" + request.userName() + "]",
            new AckedClusterStateUpdateTask<WriteUserResponse>(Priority.URGENT, request, listener) {

                private boolean alreadyExists = false;

                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    MetaData currentMetaData = currentState.metaData();
                    MetaData.Builder mdBuilder = MetaData.builder(currentMetaData);
                    alreadyExists = putUser(mdBuilder, request.userName(), request.secureHash());
                    return ClusterState.builder(currentState).metaData(mdBuilder).build();
                }

                @Override
                protected WriteUserResponse newResponse(boolean acknowledged) {
                    return new WriteUserResponse(acknowledged, alreadyExists);
                }
            });
    }

    @Override
    protected ClusterBlockException checkBlock(CreateUserRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    /**
     * Puts a user into the meta data and creates an empty privileges set.
     *
     * @return boolean true if the user already exists, otherwise false
     */
    @VisibleForTesting
    static boolean putUser(MetaData.Builder mdBuilder, String name, @Nullable SecureHash secureHash) {
        UsersMetaData oldMetaData = (UsersMetaData) mdBuilder.getCustom(UsersMetaData.TYPE);
        if (oldMetaData != null && oldMetaData.contains(name)) {
            return true;
        }
        // create a new instance of the metadata, to guarantee the cluster changed action.
        UsersMetaData newMetaData = UsersMetaData.newInstance(oldMetaData);
        newMetaData.put(name, secureHash);
        assert !newMetaData.equals(oldMetaData) : "must not be equal to guarantee the cluster change action";
        mdBuilder.putCustom(UsersMetaData.TYPE, newMetaData);

        // create empty privileges for this user
        UsersPrivilegesMetaData privilegesMetaData = UsersPrivilegesMetaData.copyOf(
            (UsersPrivilegesMetaData) mdBuilder.getCustom(UsersPrivilegesMetaData.TYPE));
        privilegesMetaData.createPrivileges(name, Collections.emptySet());
        mdBuilder.putCustom(UsersPrivilegesMetaData.TYPE, privilegesMetaData);
        return false;
    }
}
