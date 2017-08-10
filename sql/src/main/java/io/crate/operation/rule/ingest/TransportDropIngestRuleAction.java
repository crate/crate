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

package io.crate.operation.rule.ingest;

import com.google.common.annotations.VisibleForTesting;
import io.crate.metadata.rule.ingest.IngestRulesMetaData;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.MetaData.Builder;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

@Singleton
public class TransportDropIngestRuleAction extends TransportMasterNodeAction<DropIngestRuleRequest, IngestRuleResponse> {

    private static final String ACTION_NAME = "crate/sql/drop_ingest_rule";

    @Inject
    public TransportDropIngestRuleAction(Settings settings,
                                         TransportService transportService,
                                         ClusterService clusterService,
                                         ThreadPool threadPool,
                                         ActionFilters actionFilters,
                                         IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, ACTION_NAME, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver, DropIngestRuleRequest::new);
    }

    @Override
    protected String executor() {
        // no need to use a thread pool, we go async right away
        return ThreadPool.Names.SAME;
    }

    @Override
    protected IngestRuleResponse newResponse() {
        return new IngestRuleResponse();
    }

    @Override
    protected void masterOperation(DropIngestRuleRequest request, ClusterState state, ActionListener<IngestRuleResponse> listener) throws Exception {
        clusterService.submitStateUpdateTask("drop_ingest_rule", new AckedClusterStateUpdateTask<IngestRuleResponse>(Priority.IMMEDIATE, request, listener) {

            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                MetaData currentMetaData = currentState.metaData();
                Builder mdBuilder = MetaData.builder(currentMetaData);
                dropIngestRule(mdBuilder, request);
                return ClusterState.builder(currentState).metaData(mdBuilder).build();
            }

            @Override
            protected IngestRuleResponse newResponse(boolean acknowledged) {
                return new IngestRuleResponse(acknowledged);
            }
        });
    }

    @VisibleForTesting
    static void dropIngestRule(Builder mdBuilder, DropIngestRuleRequest request) {
        // create a new instance of the metadata, to guarantee the cluster changed action.
        IngestRulesMetaData newMetaData = IngestRulesMetaData.copyOf(
            (IngestRulesMetaData) mdBuilder.getCustom(IngestRulesMetaData.TYPE));
        newMetaData.dropIngestRule(request.getIngestRuleName());
        mdBuilder.putCustom(IngestRulesMetaData.TYPE, newMetaData);
    }

    @Override
    protected ClusterBlockException checkBlock(DropIngestRuleRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
