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

package io.crate.metadata;

import io.crate.Constants;
import io.crate.common.annotations.VisibleForTesting;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Objects;

/**
 * Service that ensures a "crate_defaults" template exists.
 *
 * This template matches all indices and customizes dynamic mapping updates:
 *
 *  - dynamically added strings use docValues
 */
@Singleton
public class DefaultTemplateService extends AbstractLifecycleComponent implements ClusterStateListener {

    private static final Logger LOGGER = LogManager.getLogger(DefaultTemplateService.class);

    public static final String TEMPLATE_NAME = "crate_defaults";

    private static final String DEFAULT_MAPPING_SOURCE = createDefaultMappingSource();
    private final ClusterService clusterService;

    @Inject
    public DefaultTemplateService(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    @Override
    protected void doStart() {
        clusterService.addListener(this);
    }

    @Override
    protected void doStop() {
    }

    @Override
    protected void doClose() throws IOException {
        clusterService.removeListener(this);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        createIfNotExists(event.state());
    }

    void createIfNotExists(ClusterState state) {
        DiscoveryNodes nodes = state.nodes();
        if (Objects.equals(nodes.getMasterNodeId(), nodes.getLocalNodeId()) == false) {
            return;
        }
        if (state.getMetadata().getTemplates().containsKey(TEMPLATE_NAME) == false) {
            createDefaultTemplate();
        }
    }

    private void createDefaultTemplate() {
        clusterService.submitStateUpdateTask("ensure-default-template", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                return addDefaultTemplate(currentState);
            }

            @Override
            public void onFailure(String source, Exception e) {
                LOGGER.error("Error during ensure-default-template source={}", e, source);
            }
        });
    }

    @VisibleForTesting
    static ClusterState addDefaultTemplate(ClusterState currentState) throws IOException {
        Metadata currentMetadata = currentState.getMetadata();
        ImmutableOpenMap<String, IndexTemplateMetadata> currentTemplates = currentMetadata.getTemplates();
        ImmutableOpenMap<String, IndexTemplateMetadata> newTemplates = createCopyWithDefaultTemplateAdded(currentTemplates);
        Metadata.Builder mdBuilder = Metadata.builder(currentMetadata).templates(newTemplates);
        return ClusterState.builder(currentState).metadata(mdBuilder).build();
    }

    private static ImmutableOpenMap<String, IndexTemplateMetadata> createCopyWithDefaultTemplateAdded(
        ImmutableOpenMap<String, IndexTemplateMetadata> currentTemplates) throws IOException {

        ImmutableOpenMap.Builder<String, IndexTemplateMetadata> builder = ImmutableOpenMap.builder(currentTemplates);
        builder.put(TEMPLATE_NAME, createDefaultIndexTemplateMetadata());
        return builder.build();
    }

    public static IndexTemplateMetadata createDefaultIndexTemplateMetadata() throws IOException {
        return IndexTemplateMetadata.builder(TEMPLATE_NAME)
            .order(0)
            .putMapping(Constants.DEFAULT_MAPPING_TYPE, DEFAULT_MAPPING_SOURCE)
            .patterns(Collections.singletonList("*"))
            .build();
    }

    private static String createDefaultMappingSource() {
        try {
            // @formatter:off
            XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject(Constants.DEFAULT_MAPPING_TYPE)
                .endObject()
                .endObject();
            // @formatter:on

            return Strings.toString(builder);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
