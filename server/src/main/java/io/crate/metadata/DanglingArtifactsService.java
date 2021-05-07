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

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Singleton
public class DanglingArtifactsService extends AbstractLifecycleComponent implements ClusterStateListener {

    private static final Logger LOGGER = LogManager.getLogger(DanglingArtifactsService.class);

    private final ClusterService clusterService;
    private final List<Pattern> danglingPatterns;

    @Inject
    public DanglingArtifactsService(ClusterService clusterService) {
        this.clusterService = clusterService;
        this.danglingPatterns = IndexParts.DANGLING_INDICES_PREFIX_PATTERNS
            .stream()
            .map(Pattern::compile)
            .collect(Collectors.toList());
    }

    @Override
    protected void doStart() {
        clusterService.addListener(this);
    }

    @Override
    protected void doStop() {
        clusterService.removeListener(this);
    }

    @Override
    protected void doClose() {
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (LOGGER.isInfoEnabled() && event.isNewCluster()) {
            for (ObjectCursor<String> key : event.state().metadata().indices().keys()) {
                for (Pattern pattern : danglingPatterns) {
                    if (pattern.matcher(key.value).matches()) {
                        LOGGER.info("Dangling artifacts exist in the cluster. Use 'alter cluster gc dangling artifacts;' to remove them");
                        doStop();
                        return;
                    }
                }
            }
        }
    }
}
