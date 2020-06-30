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
import io.crate.metadata.UsersMetadata;
import io.crate.user.SecureHash;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import javax.annotation.Nullable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

public class TransportAlterUserAction extends TransportMasterNodeAction<AlterUserRequest, WriteUserResponse> {

    @Inject
    public TransportAlterUserAction(TransportService transportService,
                                    ClusterService clusterService,
                                    ThreadPool threadPool,
                                    IndexNameExpressionResolver indexNameExpressionResolver) {
        super(
            "internal:crate:sql/user/alter",
            transportService,
            clusterService,
            threadPool,
            AlterUserRequest::new,
            indexNameExpressionResolver
        );
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected WriteUserResponse read(StreamInput in) throws IOException {
        return new WriteUserResponse(in);
    }

    @Override
    protected void masterOperation(AlterUserRequest request, ClusterState state, ActionListener<WriteUserResponse> listener) {
        clusterService.submitStateUpdateTask("alter_user [" + request.userName() + "]",
            new AckedClusterStateUpdateTask<WriteUserResponse>(Priority.URGENT, request, listener) {

                private boolean userExists = true;

                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    Metadata currentMetadata = currentState.metadata();
                    Metadata.Builder mdBuilder = Metadata.builder(currentMetadata);
                    userExists = alterUser(
                        mdBuilder,
                        request.userName(),
                        request.secureHash()
                    );
                    return ClusterState.builder(currentState).metadata(mdBuilder).build();
                }

                @Override
                protected WriteUserResponse newResponse(boolean acknowledged) {
                    return new WriteUserResponse(acknowledged, userExists);
                }
            });
    }

    @VisibleForTesting
    static boolean alterUser(Metadata.Builder mdBuilder, String userName, @Nullable SecureHash secureHash) {
        UsersMetadata oldMetadata = (UsersMetadata) mdBuilder.getCustom(UsersMetadata.TYPE);
        if (oldMetadata == null || !oldMetadata.contains(userName)) {
            return false;
        }
        // create a new instance of the metadata, to guarantee the cluster changed action.
        UsersMetadata newMetadata = UsersMetadata.newInstance(oldMetadata);
        newMetadata.put(userName, secureHash);

        assert !newMetadata.equals(oldMetadata) : "must not be equal to guarantee the cluster change action";
        mdBuilder.putCustom(UsersMetadata.TYPE, newMetadata);

        return true;
    }

    @Override
    protected ClusterBlockException checkBlock(AlterUserRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
