/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.license;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

@Singleton
public class TransportSetLicenseAction
    extends TransportMasterNodeAction<SetLicenseRequest, SetLicenseResponse> {

    @Inject
    public TransportSetLicenseAction(Settings settings,
                                     TransportService transportService,
                                     ClusterService clusterService,
                                     ThreadPool threadPool,
                                     ActionFilters actionFilters,
                                     IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, "crate/sql/set_license", transportService, clusterService, threadPool, actionFilters,
            indexNameExpressionResolver, SetLicenseRequest::new);
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.GENERIC;
    }

    @Override
    protected SetLicenseResponse newResponse() {
        return new SetLicenseResponse();
    }

    @Override
    protected void masterOperation(final SetLicenseRequest request,
                                   ClusterState state,
                                   ActionListener<SetLicenseResponse> listener) throws Exception {
        LicenseMetaData metaData = request.licenseMetaData();
        clusterService.submitStateUpdateTask("register license to [" + metaData.issuedTo() + "]",
            new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    MetaData.Builder mdBuilder = MetaData.builder(currentState.metaData());
                    mdBuilder.putCustom(LicenseMetaData.TYPE, metaData);
                    return ClusterState.builder(currentState).metaData(mdBuilder).build();
                }

                @Override
                public TimeValue timeout() {
                    return request.masterNodeTimeout();
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    listener.onResponse(new SetLicenseResponse(true));
                }

                @Override
                public void onFailure(String source, Exception e) {
                    listener.onFailure(e);
                }
            });
    }

    @Override
    protected ClusterBlockException checkBlock(SetLicenseRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
