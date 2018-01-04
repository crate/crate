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

public class TransportAlterUserAction extends TransportMasterNodeAction<AlterUserRequest, WriteUserResponse> {

    @Inject
    public TransportAlterUserAction(Settings settings,
                                       TransportService transportService,
                                       ClusterService clusterService,
                                       ThreadPool threadPool,
                                       ActionFilters actionFilters,
                                       IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, "crate/sql/alter_user", transportService, clusterService, threadPool, actionFilters,
            indexNameExpressionResolver, AlterUserRequest::new);
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected WriteUserResponse newResponse() {
        return new WriteUserResponse(true);
    }

    @Override
    protected void masterOperation(AlterUserRequest request, ClusterState state, ActionListener<WriteUserResponse> listener) {
        clusterService.submitStateUpdateTask("alter_user [" + request.userName() + "]",
            new AckedClusterStateUpdateTask<WriteUserResponse>(Priority.URGENT, request, listener) {

                private boolean userExists = true;

                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    MetaData currentMetaData = currentState.metaData();
                    MetaData.Builder mdBuilder = MetaData.builder(currentMetaData);
                    userExists = alterUser(
                        mdBuilder,
                        request.userName(),
                        request.secureHash()
                    );
                    return ClusterState.builder(currentState).metaData(mdBuilder).build();
                }

                @Override
                protected WriteUserResponse newResponse(boolean acknowledged) {
                    return new WriteUserResponse(acknowledged, userExists);
                }
            });
    }

    @VisibleForTesting
    static boolean alterUser(MetaData.Builder mdBuilder, String userName, @Nullable SecureHash secureHash) {
        UsersMetaData oldMetaData = (UsersMetaData) mdBuilder.getCustom(UsersMetaData.TYPE);
        if (oldMetaData == null || !oldMetaData.contains(userName)) {
            return false;
        }
        // create a new instance of the metadata, to guarantee the cluster changed action.
        UsersMetaData newMetaData = UsersMetaData.newInstance(oldMetaData);
        newMetaData.put(userName, secureHash);

        assert !newMetaData.equals(oldMetaData) : "must not be equal to guarantee the cluster change action";
        mdBuilder.putCustom(UsersMetaData.TYPE, newMetaData);

        return true;
    }

    @Override
    protected ClusterBlockException checkBlock(AlterUserRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
