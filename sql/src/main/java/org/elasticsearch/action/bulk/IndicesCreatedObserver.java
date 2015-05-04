/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package org.elasticsearch.action.bulk;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.FutureCallback;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.unit.TimeValue;

import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.TimeoutException;

class IndicesCreatedObserver {

    private IndicesCreatedObserver() {}

    public static void waitForIndicesCreated(ClusterService clusterService,
                                             ESLogger logger,
                                             Collection<String> indicesToWaitFor,
                                             final FutureCallback<Void> callback,
                                             TimeValue timeout) {
        // check if all indices exist yet
        if (clusterService.state().routingTable().indicesRouting().keySet().containsAll(indicesToWaitFor)) {
            callback.onSuccess(null);
            return;
        }

        final ClusterStateObserver observer = new ClusterStateObserver(clusterService, logger);
        observer.waitForNextChange(new ClusterStateObserver.Listener() {
            @Override
            public void onNewClusterState(ClusterState state) {
                callback.onSuccess(null);
            }

            @Override
            public void onClusterServiceClose() {
                callback.onFailure(new IllegalStateException("ClusterService closed"));
            }

            @Override
            public void onTimeout(TimeValue timeout) {
                callback.onFailure(new TimeoutException("waiting for partitions to be created timed out after " + timeout.toString()));
            }
        }, new IndicesCreatedPredicate(indicesToWaitFor), timeout);
    }

    /**
     * changepredicate that only returns true if all given indices are available in
     * the cluster state
     */
    private static class IndicesCreatedPredicate implements ClusterStateObserver.ChangePredicate {

        private final Set<String> pendingIndices;

        IndicesCreatedPredicate(Collection<String> indices) {
            this.pendingIndices = Sets.newHashSet(indices);
        }

        @Override
        public boolean apply(ClusterState previousState,
                             ClusterState.ClusterStateStatus previousStatus,
                             ClusterState newState,
                             ClusterState.ClusterStateStatus newStatus) {
            if (newStatus == ClusterState.ClusterStateStatus.APPLIED) {
                pendingIndices.removeAll(Arrays.asList(newState.metaData().concreteAllOpenIndices()));
            }
            return pendingIndices.isEmpty();
        }

        @Override
        public boolean apply(ClusterChangedEvent changedEvent) {
            pendingIndices.removeAll(changedEvent.indicesCreated());
            return pendingIndices.isEmpty();
        }
    }
}
