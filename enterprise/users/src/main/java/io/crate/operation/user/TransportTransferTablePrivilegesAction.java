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

@Singleton
public class TransportTransferTablePrivilegesAction extends TransportMasterNodeAction<TransferTablePrivilegesRequest, PrivilegesResponse> {

    private static final String ACTION_NAME = "crate/sql/transfer_table_privileges";

    @Inject
    public TransportTransferTablePrivilegesAction(Settings settings,
                                                  TransportService transportService,
                                                  ClusterService clusterService,
                                                  ThreadPool threadPool,
                                                  ActionFilters actionFilters,
                                                  IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, ACTION_NAME, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver, TransferTablePrivilegesRequest::new);
    }

    @Override
    protected String executor() {
        // no need to use a thread pool, we go async right away
        return ThreadPool.Names.SAME;
    }

    @Override
    protected PrivilegesResponse newResponse() {
        return new PrivilegesResponse();
    }

    @Override
    protected ClusterBlockException checkBlock(TransferTablePrivilegesRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected void masterOperation(TransferTablePrivilegesRequest request, ClusterState state, ActionListener<PrivilegesResponse> listener) throws Exception {
        clusterService.submitStateUpdateTask("transfer_table_privileges",
            new AckedClusterStateUpdateTask<PrivilegesResponse>(Priority.IMMEDIATE, request, listener) {

                long affectedRows = -1;

                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    MetaData currentMetaData = currentState.metaData();
                    MetaData.Builder mdBuilder = MetaData.builder(currentMetaData);
                    affectedRows = transferTablePrivileges(mdBuilder, request);
                    return ClusterState.builder(currentState).metaData(mdBuilder).build();
                }

                @Override
                protected PrivilegesResponse newResponse(boolean acknowledged) {
                    return new PrivilegesResponse(acknowledged, affectedRows);
                }
            });

    }

    @VisibleForTesting
    static long transferTablePrivileges(MetaData.Builder mdBuilder,
                                        TransferTablePrivilegesRequest request) {
        // create a new instance of the metadata, to guarantee the cluster changed action.
        UsersPrivilegesMetaData newMetaData = UsersPrivilegesMetaData.copyOf(
            (UsersPrivilegesMetaData) mdBuilder.getCustom(UsersPrivilegesMetaData.TYPE));

        long affectedRows = newMetaData.transferTablePrivileges(request.sourceIdent(), request.targetIdent());
        mdBuilder.putCustom(UsersPrivilegesMetaData.TYPE, newMetaData);
        return affectedRows;
    }
}
