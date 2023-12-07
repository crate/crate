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

import io.crate.common.annotations.VisibleForTesting;
import io.crate.user.metadata.RolesMetadata;
import io.crate.user.metadata.UsersPrivilegesMetadata;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

@Singleton
public class TransportPrivilegesAction extends TransportMasterNodeAction<PrivilegesRequest, PrivilegesResponse> {

    private static final String ACTION_NAME = "internal:crate:sql/privileges/grant";

    @Inject
    public TransportPrivilegesAction(TransportService transportService,
                                     ClusterService clusterService,
                                     ThreadPool threadPool) {
        super(
            ACTION_NAME,
            transportService,
            clusterService,
            threadPool,
            PrivilegesRequest::new
        );
    }

    @Override
    protected String executor() {
        // no need to use a thread pool, we go async right away
        return ThreadPool.Names.SAME;
    }

    @Override
    protected PrivilegesResponse read(StreamInput in) throws IOException {
        return new PrivilegesResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(PrivilegesRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected void masterOperation(PrivilegesRequest request,
                                   ClusterState state,
                                   ActionListener<PrivilegesResponse> listener) throws Exception {
        clusterService.submitStateUpdateTask("grant_privileges",
            new AckedClusterStateUpdateTask<PrivilegesResponse>(Priority.IMMEDIATE, request, listener) {

                long affectedRows = -1;
                List<String> unknownUserNames = null;

                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    Metadata currentMetadata = currentState.metadata();
                    Metadata.Builder mdBuilder = Metadata.builder(currentMetadata);
                    unknownUserNames = validateUserNames(currentMetadata, request.userNames());
                    if (unknownUserNames.isEmpty()) {
                        affectedRows = applyPrivileges(mdBuilder, request);
                    }
                    return ClusterState.builder(currentState).metadata(mdBuilder).build();
                }

                @Override
                protected PrivilegesResponse newResponse(boolean acknowledged) {
                    return new PrivilegesResponse(acknowledged, affectedRows, unknownUserNames);
                }
            });

    }

    @VisibleForTesting
    static List<String> validateUserNames(Metadata metadata, Collection<String> userNames) {
        RolesMetadata rolesMetadata = metadata.custom(RolesMetadata.TYPE);
        if (rolesMetadata == null) {
            return new ArrayList<>(userNames);
        }
        List<String> unknownUserNames = null;
        for (String userName : userNames) {
            //noinspection PointlessBooleanExpression
            if (rolesMetadata.roleNames().contains(userName) == false) {
                if (unknownUserNames == null) {
                    unknownUserNames = new ArrayList<>();
                }
                unknownUserNames.add(userName);
            }
        }
        if (unknownUserNames == null) {
            return Collections.emptyList();
        }
        return unknownUserNames;
    }

    @VisibleForTesting
    static long applyPrivileges(Metadata.Builder mdBuilder,
                                PrivilegesRequest request) {
        // create a new instance of the metadata, to guarantee the cluster changed action.
        UsersPrivilegesMetadata newMetadata = UsersPrivilegesMetadata.copyOf(
            (UsersPrivilegesMetadata) mdBuilder.getCustom(UsersPrivilegesMetadata.TYPE));

        long affectedRows = newMetadata.applyPrivileges(request.userNames(), request.privileges());
        mdBuilder.putCustom(UsersPrivilegesMetadata.TYPE, newMetadata);
        return affectedRows;
    }
}
