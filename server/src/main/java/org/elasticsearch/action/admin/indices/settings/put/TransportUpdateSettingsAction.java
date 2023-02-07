/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.indices.settings.put;

import static org.elasticsearch.common.settings.AbstractScopedSettings.ARCHIVED_SETTINGS_PREFIX;

import java.io.IOException;
import java.util.Arrays;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetadataUpdateSettingsService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportUpdateSettingsAction extends TransportMasterNodeAction<UpdateSettingsRequest, AcknowledgedResponse> {

    private final MetadataUpdateSettingsService updateSettingsService;

    @Inject
    public TransportUpdateSettingsAction(TransportService transportService,
                                         ClusterService clusterService,
                                         ThreadPool threadPool,
                                         MetadataUpdateSettingsService updateSettingsService) {
        super(UpdateSettingsAction.NAME, transportService, clusterService, threadPool, UpdateSettingsRequest::new);
        this.updateSettingsService = updateSettingsService;
    }

    @Override
    protected String executor() {
        // we go async right away....
        return ThreadPool.Names.SAME;
    }

    @Override
    protected ClusterBlockException checkBlock(UpdateSettingsRequest request, ClusterState state) {
        // allow for dedicated changes to the metadata blocks, so we don't block those to allow to "re-enable" it
        ClusterBlockException globalBlock = state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
        if (globalBlock != null) {
            return globalBlock;
        }

        // always allow removing of archived settings, so filter them out before doing further block checks
        Settings settings = request.settings().filter(k -> k.startsWith(ARCHIVED_SETTINGS_PREFIX + "*") == false);

        if (settings.size() == 1 &&  // we have to allow resetting these settings otherwise users can't unblock an index
            IndexMetadata.INDEX_BLOCKS_METADATA_SETTING.exists(settings)
            || IndexMetadata.INDEX_READ_ONLY_SETTING.exists(settings)
            || IndexMetadata.INDEX_BLOCKS_READ_ONLY_ALLOW_DELETE_SETTING.exists(settings)) {
            return null;
        }
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_WRITE, IndexNameExpressionResolver.concreteIndexNames(state, request));
    }

    @Override
    protected AcknowledgedResponse read(StreamInput in) throws IOException {
        return new AcknowledgedResponse(in);
    }

    @Override
    protected void masterOperation(final UpdateSettingsRequest request,
                                   final ClusterState state,
                                   final ActionListener<AcknowledgedResponse> listener) {
        updateSettingsService.updateSettings(request, new ActionListener<ClusterStateUpdateResponse>() {
            @Override
            public void onResponse(ClusterStateUpdateResponse response) {
                listener.onResponse(new AcknowledgedResponse(response.isAcknowledged()));
            }

            @Override
            public void onFailure(Exception t) {
                logger.debug(() -> new ParameterizedMessage("failed to update settings on indices [{}]", Arrays.toString(request.indices())), t);
                listener.onFailure(t);
            }
        });
    }
}
