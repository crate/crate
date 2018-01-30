/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;

import java.util.concurrent.CompletableFuture;

import static org.elasticsearch.cluster.ClusterState.UNKNOWN_UUID;

@Singleton
public class ClusterIdService extends AbstractLifecycleComponent implements ClusterStateListener {

    private final ClusterService clusterService;
    private volatile CompletableFuture<String> clusterIdFuture = new CompletableFuture<>();


    @Inject
    public ClusterIdService(Settings settings, ClusterService clusterService) {
        super(settings);
        this.clusterService = clusterService;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (logger.isTraceEnabled()) {
            logger.trace("[{}] Receiving new cluster state, reason {}",
                clusterService.state().nodes().getLocalNodeId(), event.source());
        }
        String clusterUUID = event.state().getMetaData().clusterUUID();
        if (UNKNOWN_UUID.equals(clusterUUID)) {
            return;
        }
        if (clusterIdFuture.isDone()) {
            // Replacing an existing cluster UUID
            clusterIdFuture = CompletableFuture.completedFuture(clusterUUID);
        } else {
            clusterIdFuture.complete(clusterUUID);
        }
    }

    /**
     * return a CompletableFuture that is available once the clusterId is set
     */
    public CompletableFuture<String> clusterId() {
        return clusterIdFuture;
    }

    @Override
    protected void doStart() {
        // add listener here to avoid guice proxy errors if the ClusterService could not be build
        clusterService.addListener(this);
    }

    @Override
    protected void doStop() {
        clusterService.removeListener(this);
    }

    @Override
    protected void doClose() {
    }
}
