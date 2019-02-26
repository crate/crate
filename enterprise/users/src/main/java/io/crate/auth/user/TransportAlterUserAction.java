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

import com.google.common.annotations.VisibleForTesting;
import io.crate.metadata.UsersMetaData;
import io.crate.user.SecureHash;
import io.crate.es.action.ActionListener;
import io.crate.es.action.support.master.TransportMasterNodeAction;
import io.crate.es.cluster.AckedClusterStateUpdateTask;
import io.crate.es.cluster.ClusterState;
import io.crate.es.cluster.block.ClusterBlockException;
import io.crate.es.cluster.block.ClusterBlockLevel;
import io.crate.es.cluster.metadata.IndexNameExpressionResolver;
import io.crate.es.cluster.metadata.MetaData;
import io.crate.es.cluster.service.ClusterService;
import io.crate.es.common.Nullable;
import io.crate.es.common.Priority;
import io.crate.es.common.inject.Inject;
import io.crate.es.common.settings.Settings;
import io.crate.es.threadpool.ThreadPool;
import io.crate.es.transport.TransportService;

public class TransportAlterUserAction extends TransportMasterNodeAction<AlterUserRequest, WriteUserResponse> {

    @Inject
    public TransportAlterUserAction(Settings settings,
                                       TransportService transportService,
                                       ClusterService clusterService,
                                       ThreadPool threadPool,
                                       IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, "internal:crate:sql/user/alter", transportService, clusterService, threadPool,
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
