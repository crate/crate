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

package io.crate.executor.transport.ddl;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Arrays;

@Singleton
public class TransportRenameTableAction extends TransportMasterNodeAction<RenameTableRequest, RenameTableResponse> {

    private static final String ACTION_NAME = "crate/table/rename";
    private static final IndicesOptions INDICES_OPTIONS = IndicesOptions.fromOptions(false, false, false, false);

    @Inject
    public TransportRenameTableAction(Settings settings,
                                      TransportService transportService,
                                      ClusterService clusterService,
                                      ThreadPool threadPool,
                                      ActionFilters actionFilters,
                                      IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, ACTION_NAME, transportService, clusterService, threadPool, actionFilters,
            indexNameExpressionResolver, RenameTableRequest::new);
    }

    @Override
    protected String executor() {
        // no need to use a thread pool, we go async right away
        return ThreadPool.Names.SAME;
    }

    @Override
    protected RenameTableResponse newResponse() {
        return new RenameTableResponse();
    }

    @Override
    protected void masterOperation(RenameTableRequest request,
                                   ClusterState state,
                                   ActionListener<RenameTableResponse> listener) throws Exception {
        final Index[] concreteIndices = indexNameExpressionResolver.concreteIndices(state, INDICES_OPTIONS,
            request.sourceIndices());
        String[] targetIndexNames = request.targetIndices();

        updateClusterState(concreteIndices, targetIndexNames, request, new ActionListener<ClusterStateUpdateResponse>() {

            @Override
            public void onResponse(ClusterStateUpdateResponse response) {
                listener.onResponse(new RenameTableResponse(response.isAcknowledged()));
            }

            @Override
            public void onFailure(Exception t) {
                logger.debug((Supplier<?>) () -> new ParameterizedMessage("failed to rename indices [{}]", (Object) concreteIndices), t);
                listener.onFailure(t);
            }
        });
    }

    private void updateClusterState(Index[] concreteIndices,
                                    String[] targetIndexNames,
                                    RenameTableRequest request,
                                    ActionListener<ClusterStateUpdateResponse> listener) {
        final String indicesAsString = Arrays.toString(concreteIndices);
        clusterService.submitStateUpdateTask("rename-indices " + indicesAsString,
            new AckedClusterStateUpdateTask<ClusterStateUpdateResponse>(Priority.URGENT, request, listener) {
                @Override
                protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                    return new ClusterStateUpdateResponse(acknowledged);
                }

                @Override
                public ClusterState execute(ClusterState currentState) {
                    logger.info("renaming indices [{}]", indicesAsString);

                    MetaData.Builder mdBuilder = MetaData.builder(currentState.metaData());
                    ClusterBlocks.Builder blocksBuilder = ClusterBlocks.builder()
                        .blocks(currentState.blocks());

                    for (int i = 0; i < concreteIndices.length; i++) {
                        Index index = concreteIndices[i];
                        IndexMetaData indexMetaData = currentState.metaData().getIndexSafe(index);
                        IndexMetaData targetIndexMetadata = IndexMetaData.builder(indexMetaData)
                            .index(targetIndexNames[i]).build();
                        mdBuilder.remove(index.getName());
                        mdBuilder.put(targetIndexMetadata, true);
                        blocksBuilder.removeIndexBlocks(index.getName());
                        blocksBuilder.addBlocks(targetIndexMetadata);
                    }

                    return ClusterState.builder(currentState).metaData(mdBuilder).blocks(blocksBuilder).build();
                }
            });
    }

    @Override
    protected ClusterBlockException checkBlock(RenameTableRequest request, ClusterState state) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_WRITE,
            indexNameExpressionResolver.concreteIndexNames(state, INDICES_OPTIONS, request.sourceIndices()));
    }
}
