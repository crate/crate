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

package org.elasticsearch.action.admin.cluster.node.reload;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.BaseNodeRequest;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.KeyStoreWrapper;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.plugins.ReloadablePlugin;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TransportNodesReloadSecureSettingsAction extends TransportNodesAction<NodesReloadSecureSettingsRequest,
                                                                    NodesReloadSecureSettingsResponse,
                                                                    TransportNodesReloadSecureSettingsAction.NodeRequest,
                                                                    NodesReloadSecureSettingsResponse.NodeResponse> {

    private final Environment environment;
    private final PluginsService pluginsService;

    @Inject
    public TransportNodesReloadSecureSettingsAction(Settings settings, ThreadPool threadPool, ClusterService clusterService,
                                      TransportService transportService, ActionFilters actionFilters,
                                      IndexNameExpressionResolver indexNameExpressionResolver, Environment environment,
                                      PluginsService pluginService) {
        super(settings, NodesReloadSecureSettingsAction.NAME, threadPool, clusterService, transportService, actionFilters,
                indexNameExpressionResolver, NodesReloadSecureSettingsRequest::new, NodeRequest::new, ThreadPool.Names.GENERIC,
                NodesReloadSecureSettingsResponse.NodeResponse.class);
        this.environment = environment;
        this.pluginsService = pluginService;
    }

    @Override
    protected NodesReloadSecureSettingsResponse newResponse(NodesReloadSecureSettingsRequest request,
                                                            List<NodesReloadSecureSettingsResponse.NodeResponse> responses,
                                                            List<FailedNodeException> failures) {
        return new NodesReloadSecureSettingsResponse(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected NodeRequest newNodeRequest(String nodeId, NodesReloadSecureSettingsRequest request) {
        return new NodeRequest(nodeId, request);
    }

    @Override
    protected NodesReloadSecureSettingsResponse.NodeResponse newNodeResponse() {
        return new NodesReloadSecureSettingsResponse.NodeResponse();
    }

    @Override
    protected NodesReloadSecureSettingsResponse.NodeResponse nodeOperation(NodeRequest nodeReloadRequest) {
        try (KeyStoreWrapper keystore = KeyStoreWrapper.load(environment.configFile())) {
            // reread keystore from config file
            if (keystore == null) {
                return new NodesReloadSecureSettingsResponse.NodeResponse(clusterService.localNode(),
                        new IllegalStateException("Keystore is missing"));
            }
            keystore.decrypt(new char[0]);
            // add the keystore to the original node settings object
            final Settings settingsWithKeystore = Settings.builder()
                    .put(environment.settings(), false)
                    .setSecureSettings(keystore)
                    .build();
            final List<Exception> exceptions = new ArrayList<>();
            // broadcast the new settings object (with the open embedded keystore) to all reloadable plugins
            pluginsService.filterPlugins(ReloadablePlugin.class).stream().forEach(p -> {
                try {
                    p.reload(settingsWithKeystore);
                } catch (final Exception e) {
                    logger.warn((Supplier<?>) () -> new ParameterizedMessage("Reload failed for plugin [{}]", p.getClass().getSimpleName()),
                            e);
                    exceptions.add(e);
                }
            });
            ExceptionsHelper.rethrowAndSuppress(exceptions);
            return new NodesReloadSecureSettingsResponse.NodeResponse(clusterService.localNode(), null);
        } catch (final Exception e) {
            return new NodesReloadSecureSettingsResponse.NodeResponse(clusterService.localNode(), e);
        }
    }

    public static class NodeRequest extends BaseNodeRequest {

        NodesReloadSecureSettingsRequest request;

        public NodeRequest() {
        }

        NodeRequest(String nodeId, NodesReloadSecureSettingsRequest request) {
            super(nodeId);
            this.request = request;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            request = new NodesReloadSecureSettingsRequest();
            request.readFrom(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            request.writeTo(out);
        }
    }
}
