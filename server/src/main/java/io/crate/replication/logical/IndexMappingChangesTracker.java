/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.replication.logical;

import io.crate.common.unit.TimeValue;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;
import java.util.function.Function;

public class IndexMappingChangesTracker implements Closeable {

    private static final Logger LOGGER = Loggers.getLogger(IndexMappingChangesTracker.class);

    private final ThreadPool threadPool;
    private final Function<String, Client> remoteClient;
    public static final long REMOTE_CLUSTER_REPO_REQ_TIMEOUT_IN_MILLI_SEC = 60000L;

    public IndexMappingChangesTracker(ThreadPool threadPool, Function<String, Client> remoteClient) {
        this.threadPool = threadPool;
        this.remoteClient = remoteClient;
    }

    public void start(String clusterName) {
        LOGGER.debug("Schedule tracking for remote cluster state");
        threadPool.scheduleWithFixedDelay(() -> trackMapping(clusterName), TimeValue.timeValueSeconds(1), ThreadPool.Names.SAME);
    }

    private void trackMapping(String clusterName) {
        LOGGER.debug("Start tracking for remote cluster state");
        getRemoteClusterState(clusterName, false, false, new ActionListener<>() {
            @Override
            public void onResponse(ClusterState remoteClusterState) {
                LOGGER.debug("Fetched remote cluster state {}", remoteClusterState.term());
            }
            @Override
            public void onFailure(Exception e) {
                LOGGER.error(e);
            }
        });
    }


    private void getRemoteClusterState(
        String clusterName,
        boolean includeNodes,
        boolean includeRouting,
        ActionListener<ClusterState> listener) {
        var clusterStateRequest = remoteClient.apply(clusterName).admin().cluster().prepareState()
            .clear()
            .setMetadata(true)
            .setNodes(includeNodes)
            .setRoutingTable(includeRouting)
            .setIndicesOptions(IndicesOptions.strictSingleIndexNoExpandForbidClosed())
            .setWaitForTimeOut(new TimeValue(REMOTE_CLUSTER_REPO_REQ_TIMEOUT_IN_MILLI_SEC))
            .request();
        remoteClient.apply(clusterName).admin().cluster().execute(
            ClusterStateAction.INSTANCE, clusterStateRequest, new ActionListener<>() {
                @Override
                public void onResponse(ClusterStateResponse clusterStateResponse) {
                    ClusterState remoteState = clusterStateResponse.getState();
                    LOGGER.trace("Successfully fetched the cluster state from remote repository {}", remoteState);
                    listener.onResponse(clusterStateResponse.getState());
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
    }

    @Override
    public void close() throws IOException {

    }
}
