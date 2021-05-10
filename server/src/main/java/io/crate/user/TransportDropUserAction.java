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

import com.google.common.annotations.VisibleForTesting;
import io.crate.user.metadata.UsersMetadata;
import io.crate.user.metadata.UsersPrivilegesMetadata;
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

import javax.annotation.Nullable;
import java.io.IOException;

public class TransportDropUserAction extends TransportMasterNodeAction<DropUserRequest, WriteUserResponse> {

    @Inject
    public TransportDropUserAction(TransportService transportService,
                                   ClusterService clusterService,
                                   ThreadPool threadPool,
                                   IndexNameExpressionResolver indexNameExpressionResolver) {
        super(
            "internal:crate:sql/user/drop",
            transportService,
            clusterService,
            threadPool,
            DropUserRequest::new,
            indexNameExpressionResolver
        );
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.GENERIC;
    }

    @Override
    protected WriteUserResponse read(StreamInput in) throws IOException {
        return new WriteUserResponse(in);
    }

    @Override
    protected void masterOperation(Task task,
                                   DropUserRequest request,
                                   ClusterState state,
                                   ActionListener<WriteUserResponse> listener) throws Exception {
        clusterService.submitStateUpdateTask("drop_user [" + request.userName() + "]",
            new AckedClusterStateUpdateTask<WriteUserResponse>(Priority.URGENT, request, listener) {

                private boolean alreadyExists = true;

                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    Metadata currentMetadata = currentState.metadata();
                    Metadata.Builder mdBuilder = Metadata.builder(currentMetadata);
                    alreadyExists = dropUser(
                        mdBuilder,
                        currentMetadata.custom(UsersMetadata.TYPE),
                        request.userName()
                    );
                    return ClusterState.builder(currentState).metadata(mdBuilder).build();
                }

                @Override
                protected WriteUserResponse newResponse(boolean acknowledged) {
                    return new WriteUserResponse(acknowledged, alreadyExists);
                }
            });
    }

    @Override
    protected ClusterBlockException checkBlock(DropUserRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @VisibleForTesting
    static boolean dropUser(Metadata.Builder mdBuilder,
                            @Nullable UsersMetadata oldMetadata,
                            String name) {
        if (oldMetadata == null || oldMetadata.contains(name) == false) {
            return false;
        }
        // create a new instance of the metadata, to guarantee the cluster changed action.
        UsersMetadata newMetadata = UsersMetadata.newInstance(oldMetadata);
        newMetadata.remove(name);

        assert !newMetadata.equals(oldMetadata) : "must not be equal to guarantee the cluster change action";
        mdBuilder.putCustom(UsersMetadata.TYPE, newMetadata);

        // removes all privileges for this user
        UsersPrivilegesMetadata privilegesMetadata = UsersPrivilegesMetadata.copyOf(
            (UsersPrivilegesMetadata) mdBuilder.getCustom(UsersPrivilegesMetadata.TYPE));
        privilegesMetadata.dropPrivileges(name);
        mdBuilder.putCustom(UsersPrivilegesMetadata.TYPE, privilegesMetadata);

        return true;
    }
}
