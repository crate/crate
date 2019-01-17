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

package org.elasticsearch.action.admin.cluster.health;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.LocalClusterUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.gateway.GatewayAllocator;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.function.Predicate;

public class TransportClusterHealthAction extends TransportMasterNodeReadAction<ClusterHealthRequest, ClusterHealthResponse> {

    private final GatewayAllocator gatewayAllocator;

    @Inject
    public TransportClusterHealthAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                        ThreadPool threadPool, ActionFilters actionFilters,
                                        IndexNameExpressionResolver indexNameExpressionResolver, GatewayAllocator gatewayAllocator) {
        super(settings, ClusterHealthAction.NAME, false, transportService, clusterService, threadPool, actionFilters,
            indexNameExpressionResolver, ClusterHealthRequest::new);
        this.gatewayAllocator = gatewayAllocator;
    }

    @Override
    protected String executor() {
        // this should be executing quickly no need to fork off
        return ThreadPool.Names.SAME;
    }

    @Override
    protected ClusterBlockException checkBlock(ClusterHealthRequest request, ClusterState state) {
        return null; // we want users to be able to call this even when there are global blocks, just to check the health (are there blocks?)
    }

    @Override
    protected ClusterHealthResponse newResponse() {
        return new ClusterHealthResponse();
    }

    @Override
    protected final void masterOperation(ClusterHealthRequest request, ClusterState state, ActionListener<ClusterHealthResponse> listener) throws Exception {
        logger.warn("attempt to execute a cluster health operation without a task");
        throw new UnsupportedOperationException("task parameter is required for this operation");
    }

    @Override
    protected void masterOperation(Task task, final ClusterHealthRequest request, final ClusterState unusedState, final ActionListener<ClusterHealthResponse> listener) {
        if (request.waitForEvents() != null) {
            final long endTimeMS = TimeValue.nsecToMSec(System.nanoTime()) + request.timeout().millis();
            if (request.local()) {
                clusterService.submitStateUpdateTask("cluster_health (wait_for_events [" + request.waitForEvents() + "])", new LocalClusterUpdateTask(request.waitForEvents()) {
                    @Override
                    public ClusterTasksResult<LocalClusterUpdateTask> execute(ClusterState currentState) {
                        return unchanged();
                    }

                    @Override
                    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                        final long timeoutInMillis = Math.max(0, endTimeMS - TimeValue.nsecToMSec(System.nanoTime()));
                        final TimeValue newTimeout = TimeValue.timeValueMillis(timeoutInMillis);
                        request.timeout(newTimeout);
                        executeHealth(request, listener);
                    }

                    @Override
                    public void onFailure(String source, Exception e) {
                        logger.error(() -> new ParameterizedMessage("unexpected failure during [{}]", source), e);
                        listener.onFailure(e);
                    }
                });
            } else {
                clusterService.submitStateUpdateTask("cluster_health (wait_for_events [" + request.waitForEvents() + "])", new ClusterStateUpdateTask(request.waitForEvents()) {
                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        return currentState;
                    }

                    @Override
                    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                        final long timeoutInMillis = Math.max(0, endTimeMS - TimeValue.nsecToMSec(System.nanoTime()));
                        final TimeValue newTimeout = TimeValue.timeValueMillis(timeoutInMillis);
                        request.timeout(newTimeout);
                        executeHealth(request, listener);
                    }

                    @Override
                    public void onNoLongerMaster(String source) {
                        logger.trace("stopped being master while waiting for events with priority [{}]. retrying.", request.waitForEvents());
                        // TransportMasterNodeAction implements the retry logic, which is triggered by passing a NotMasterException
                        listener.onFailure(new NotMasterException("no longer master. source: [" + source + "]"));
                    }

                    @Override
                    public void onFailure(String source, Exception e) {
                        logger.error(() -> new ParameterizedMessage("unexpected failure during [{}]", source), e);
                        listener.onFailure(e);
                    }
                });
            }
        } else {
            executeHealth(request, listener);
        }

    }

    private void executeHealth(final ClusterHealthRequest request, final ActionListener<ClusterHealthResponse> listener) {
        int waitFor = 0;
        if (request.waitForStatus() != null) {
            waitFor++;
        }
        if (request.waitForNoRelocatingShards()) {
            waitFor++;
        }
        if (request.waitForNoInitializingShards()) {
            waitFor++;
        }
        if (request.waitForActiveShards().equals(ActiveShardCount.NONE) == false) {
            waitFor++;
        }
        if (request.waitForNodes().isEmpty() == false) {
            waitFor++;
        }
        if (request.indices() != null && request.indices().length > 0) { // check that they actually exists in the meta data
            waitFor++;
        }

        final ClusterState state = clusterService.state();
        final ClusterStateObserver observer = new ClusterStateObserver(state, clusterService, null, logger, threadPool.getThreadContext());
        if (request.timeout().millis() == 0) {
            listener.onResponse(getResponse(request, state, waitFor, request.timeout().millis() == 0));
            return;
        }
        final int concreteWaitFor = waitFor;
        final Predicate<ClusterState> validationPredicate = newState -> validateRequest(request, newState, concreteWaitFor);

        final ClusterStateObserver.Listener stateListener = new ClusterStateObserver.Listener() {
            @Override
            public void onNewClusterState(ClusterState clusterState) {
                listener.onResponse(getResponse(request, clusterState, concreteWaitFor, false));
            }

            @Override
            public void onClusterServiceClose() {
                listener.onFailure(new IllegalStateException("ClusterService was close during health call"));
            }

            @Override
            public void onTimeout(TimeValue timeout) {
                final ClusterHealthResponse response = getResponse(request, observer.setAndGetObservedState(), concreteWaitFor, true);
                listener.onResponse(response);
            }
        };
        if (validationPredicate.test(state)) {
            stateListener.onNewClusterState(state);
        } else {
            observer.waitForNextChange(stateListener, validationPredicate, request.timeout());
        }
    }

    private boolean validateRequest(final ClusterHealthRequest request, ClusterState clusterState, final int waitFor) {
        ClusterHealthResponse response = clusterHealth(request, clusterState, clusterService.getMasterService().numberOfPendingTasks(),
                gatewayAllocator.getNumberOfInFlightFetch(), clusterService.getMasterService().getMaxTaskWaitTime());
        int readyCounter = prepareResponse(request, response, clusterState, indexNameExpressionResolver);
        return readyCounter == waitFor;
    }

    private ClusterHealthResponse getResponse(final ClusterHealthRequest request, ClusterState clusterState, final int waitFor, boolean timedOut) {
        ClusterHealthResponse response = clusterHealth(request, clusterState, clusterService.getMasterService().numberOfPendingTasks(),
                gatewayAllocator.getNumberOfInFlightFetch(), clusterService.getMasterService().getMaxTaskWaitTime());
        int readyCounter = prepareResponse(request, response, clusterState, indexNameExpressionResolver);
        boolean valid = (readyCounter == waitFor);
        assert valid || timedOut;
        // we check for a timeout here since this method might be called from the wait_for_events
        // response handler which might have timed out already.
        // if the state is sufficient for what we where waiting for we don't need to mark this as timedOut.
        // We spend too much time in waiting for events such that we might already reached a valid state.
        // this should not mark the request as timed out
        response.setTimedOut(timedOut && valid == false);
        return response;
    }

    static int prepareResponse(final ClusterHealthRequest request, final ClusterHealthResponse response,
                                   final ClusterState clusterState, final IndexNameExpressionResolver indexNameExpressionResolver) {
        int waitForCounter = 0;
        if (request.waitForStatus() != null && response.getStatus().value() <= request.waitForStatus().value()) {
            waitForCounter++;
        }
        if (request.waitForNoRelocatingShards() && response.getRelocatingShards() == 0) {
            waitForCounter++;
        }
        if (request.waitForNoInitializingShards() && response.getInitializingShards() == 0) {
            waitForCounter++;
        }
        if (request.waitForActiveShards().equals(ActiveShardCount.NONE) == false) {
            ActiveShardCount waitForActiveShards = request.waitForActiveShards();
            assert waitForActiveShards.equals(ActiveShardCount.DEFAULT) == false :
                "waitForActiveShards must not be DEFAULT on the request object, instead it should be NONE";
            if (waitForActiveShards.equals(ActiveShardCount.ALL)) {
                if (response.getUnassignedShards() == 0 && response.getInitializingShards() == 0) {
                    // if we are waiting for all shards to be active, then the num of unassigned and num of initializing shards must be 0
                    waitForCounter++;
                }
            } else if (waitForActiveShards.enoughShardsActive(response.getActiveShards())) {
                // there are enough active shards to meet the requirements of the request
                waitForCounter++;
            }
        }
        if (request.indices() != null && request.indices().length > 0) {
            try {
                indexNameExpressionResolver.concreteIndexNames(clusterState, IndicesOptions.strictExpand(), request.indices());
                waitForCounter++;
            } catch (IndexNotFoundException e) {
                response.setStatus(ClusterHealthStatus.RED); // no indices, make sure its RED
                // missing indices, wait a bit more...
            }
        }
        if (!request.waitForNodes().isEmpty()) {
            if (request.waitForNodes().startsWith(">=")) {
                int expected = Integer.parseInt(request.waitForNodes().substring(2));
                if (response.getNumberOfNodes() >= expected) {
                    waitForCounter++;
                }
            } else if (request.waitForNodes().startsWith("ge(")) {
                int expected = Integer.parseInt(request.waitForNodes().substring(3, request.waitForNodes().length() - 1));
                if (response.getNumberOfNodes() >= expected) {
                    waitForCounter++;
                }
            } else if (request.waitForNodes().startsWith("<=")) {
                int expected = Integer.parseInt(request.waitForNodes().substring(2));
                if (response.getNumberOfNodes() <= expected) {
                    waitForCounter++;
                }
            } else if (request.waitForNodes().startsWith("le(")) {
                int expected = Integer.parseInt(request.waitForNodes().substring(3, request.waitForNodes().length() - 1));
                if (response.getNumberOfNodes() <= expected) {
                    waitForCounter++;
                }
            } else if (request.waitForNodes().startsWith(">")) {
                int expected = Integer.parseInt(request.waitForNodes().substring(1));
                if (response.getNumberOfNodes() > expected) {
                    waitForCounter++;
                }
            } else if (request.waitForNodes().startsWith("gt(")) {
                int expected = Integer.parseInt(request.waitForNodes().substring(3, request.waitForNodes().length() - 1));
                if (response.getNumberOfNodes() > expected) {
                    waitForCounter++;
                }
            } else if (request.waitForNodes().startsWith("<")) {
                int expected = Integer.parseInt(request.waitForNodes().substring(1));
                if (response.getNumberOfNodes() < expected) {
                    waitForCounter++;
                }
            } else if (request.waitForNodes().startsWith("lt(")) {
                int expected = Integer.parseInt(request.waitForNodes().substring(3, request.waitForNodes().length() - 1));
                if (response.getNumberOfNodes() < expected) {
                    waitForCounter++;
                }
            } else {
                int expected = Integer.parseInt(request.waitForNodes());
                if (response.getNumberOfNodes() == expected) {
                    waitForCounter++;
                }
            }
        }
        return waitForCounter;
    }


    private ClusterHealthResponse clusterHealth(ClusterHealthRequest request, ClusterState clusterState, int numberOfPendingTasks, int numberOfInFlightFetch,
                                                TimeValue pendingTaskTimeInQueue) {
        if (logger.isTraceEnabled()) {
            logger.trace("Calculating health based on state version [{}]", clusterState.version());
        }

        String[] concreteIndices;
        try {
            concreteIndices = indexNameExpressionResolver.concreteIndexNames(clusterState, request);
        } catch (IndexNotFoundException e) {
            // one of the specified indices is not there - treat it as RED.
            ClusterHealthResponse response = new ClusterHealthResponse(clusterState.getClusterName().value(), Strings.EMPTY_ARRAY, clusterState,
                    numberOfPendingTasks, numberOfInFlightFetch, UnassignedInfo.getNumberOfDelayedUnassigned(clusterState),
                    pendingTaskTimeInQueue);
            response.setStatus(ClusterHealthStatus.RED);
            return response;
        }

        return new ClusterHealthResponse(clusterState.getClusterName().value(), concreteIndices, clusterState, numberOfPendingTasks,
                numberOfInFlightFetch, UnassignedInfo.getNumberOfDelayedUnassigned(clusterState), pendingTaskTimeInQueue);
    }
}
