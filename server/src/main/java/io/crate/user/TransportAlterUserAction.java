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

import java.io.IOException;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import io.crate.user.AlterUserRequest;
import io.crate.user.WriteUserResponse;
import io.crate.user.metadata.UsersMetadata;

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
    protected void masterOperation(Task task,
                                   AlterUserRequest request,
                                   ClusterState state,
                                   ActionListener<WriteUserResponse> listener) {
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
