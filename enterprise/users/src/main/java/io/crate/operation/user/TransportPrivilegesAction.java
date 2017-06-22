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
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

@Singleton
public class TransportPrivilegesAction extends TransportMasterNodeAction<PrivilegesRequest, ApplyPrivilegesResponse> {

    private static final String ACTION_NAME = "crate/sql/grant_privileges";

    @Inject
    public TransportPrivilegesAction(Settings settings,
                                     TransportService transportService,
                                     ClusterService clusterService,
                                     ThreadPool threadPool,
                                     ActionFilters actionFilters,
                                     IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, ACTION_NAME, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver, PrivilegesRequest::new);
    }

    @Override
    protected String executor() {
        // no need to use a thread pool, we go async right away
        return ThreadPool.Names.SAME;
    }

    @Override
    protected ApplyPrivilegesResponse newResponse() {
        return new ApplyPrivilegesResponse();
    }

    @Override
    protected ClusterBlockException checkBlock(PrivilegesRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected void masterOperation(PrivilegesRequest request, ClusterState state, ActionListener<ApplyPrivilegesResponse> listener) throws Exception {
        clusterService.submitStateUpdateTask("grant_privileges",
            new AckedClusterStateUpdateTask<ApplyPrivilegesResponse>(Priority.IMMEDIATE, request, listener) {

                long affectedRows = -1;
                List<String> unknownUserNames = null;

                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    MetaData currentMetaData = currentState.metaData();
                    MetaData.Builder mdBuilder = MetaData.builder(currentMetaData);
                    unknownUserNames = validateUserNames(currentMetaData, request.userNames());
                    if (unknownUserNames.isEmpty()) {
                        affectedRows = applyPrivileges(mdBuilder, request);
                    }
                    return ClusterState.builder(currentState).metaData(mdBuilder).build();
                }

                @Override
                protected ApplyPrivilegesResponse newResponse(boolean acknowledged) {
                    return new ApplyPrivilegesResponse(acknowledged, affectedRows, unknownUserNames);
                }
            });

    }

    @VisibleForTesting
    static List<String> validateUserNames(MetaData metaData, Collection<String> userNames) {
        UsersMetaData usersMetaData = metaData.custom(UsersMetaData.TYPE);
        if (usersMetaData == null) {
            return new ArrayList<>(userNames);
        }
        List<String> unknownUserNames = null;
        for (String userName : userNames) {
            //noinspection PointlessBooleanExpression
            if (usersMetaData.users().contains(userName) == false) {
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
    static long applyPrivileges(MetaData.Builder mdBuilder,
                                PrivilegesRequest request) {
        // create a new instance of the metadata, to guarantee the cluster changed action.
        UsersPrivilegesMetaData newMetaData = UsersPrivilegesMetaData.copyOf(
            (UsersPrivilegesMetaData) mdBuilder.getCustom(UsersPrivilegesMetaData.TYPE));

        long affectedRows = newMetaData.applyPrivileges(request.userNames(), request.privileges());
        mdBuilder.putCustom(UsersPrivilegesMetaData.TYPE, newMetaData);
        return affectedRows;
    }
}
