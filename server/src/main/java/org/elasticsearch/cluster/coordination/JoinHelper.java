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

package org.elasticsearch.cluster.coordination;

import static org.elasticsearch.monitor.StatusInfo.Status.UNHEALTHY;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskConfig;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.coordination.Coordinator.Mode;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.monitor.NodeHealthService;
import org.elasticsearch.monitor.StatusInfo;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPool.Names;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponse.Empty;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;
import org.jetbrains.annotations.Nullable;

import io.crate.common.collections.Tuple;
import io.crate.common.unit.TimeValue;

public class JoinHelper {

    private static final Logger LOGGER = LogManager.getLogger(JoinHelper.class);

    public static final String JOIN_ACTION_NAME = "internal:cluster/coordination/join";
    public static final String VALIDATE_JOIN_ACTION_NAME = "internal:cluster/coordination/join/validate";
    public static final String START_JOIN_ACTION_NAME = "internal:cluster/coordination/start_join";

    // the timeout for each join attempt
    public static final Setting<TimeValue> JOIN_TIMEOUT_SETTING =
        Setting.timeSetting("cluster.join.timeout",
            TimeValue.timeValueMillis(60000), TimeValue.timeValueMillis(1), Setting.Property.NodeScope);

    private final MasterService masterService;
    private final TransportService transportService;
    private volatile JoinTaskExecutor joinTaskExecutor;

    @Nullable // if using single-node discovery
    private final TimeValue joinTimeout;
    private final NodeHealthService nodeHealthService;

    private final Set<Tuple<DiscoveryNode, JoinRequest>> pendingOutgoingJoins = Collections.synchronizedSet(new HashSet<>());

    private final AtomicReference<FailedJoinAttempt> lastFailedJoinAttempt = new AtomicReference<>();

    private final Supplier<JoinTaskExecutor> joinTaskExecutorGenerator;


    JoinHelper(Settings settings,
               AllocationService allocationService,
               MasterService masterService,
               TransportService transportService,
               LongSupplier currentTermSupplier,
               Supplier<ClusterState> currentStateSupplier,
               BiConsumer<JoinRequest, ActionListener<Void>> joinHandler,
               Function<StartJoinRequest, Join> joinLeaderInTerm,
               Collection<BiConsumer<DiscoveryNode, ClusterState>> joinValidators,
               RerouteService rerouteService,
               NodeHealthService nodeHealthService) {
        this.masterService = masterService;
        this.transportService = transportService;
        this.nodeHealthService = nodeHealthService;
        this.joinTimeout = DiscoveryModule.isSingleNodeDiscovery(settings) ? null : JOIN_TIMEOUT_SETTING.get(settings);
        this.joinTaskExecutorGenerator = () -> new JoinTaskExecutor(allocationService, LOGGER, rerouteService) {

            private final long term = currentTermSupplier.getAsLong();

            @Override
            public ClusterTasksResult<JoinTaskExecutor.Task> execute(ClusterState currentState, List<JoinTaskExecutor.Task> joiningTasks)
                throws Exception {
                // The current state that MasterService uses might have been updated by a (different) master in a higher term already
                // Stop processing the current cluster state update, as there's no point in continuing to compute it as
                // it will later be rejected by Coordinator.publish(...) anyhow
                if (currentState.term() > term) {
                    LOGGER.trace("encountered higher term {} than current {}, there is a newer master", currentState.term(), term);
                    throw new NotMasterException("Higher term encountered (current: " + currentState.term() + " > used: " +
                        term + "), there is a newer master");
                } else if (currentState.nodes().getMasterNodeId() == null && joiningTasks.stream().anyMatch(Task::isBecomeMasterTask)) {
                    assert currentState.term() < term : "there should be at most one become master task per election (= by term)";
                    final CoordinationMetadata coordinationMetadata =
                            CoordinationMetadata.builder(currentState.coordinationMetadata()).term(term).build();
                    final Metadata metadata = Metadata.builder(currentState.metadata()).coordinationMetadata(coordinationMetadata).build();
                    currentState = ClusterState.builder(currentState).metadata(metadata).build();
                } else if (currentState.nodes().isLocalNodeElectedMaster()) {
                    assert currentState.term() == term : "term should be stable for the same master";
                }
                return super.execute(currentState, joiningTasks);
            }

        };

        transportService.registerRequestHandler(JOIN_ACTION_NAME, ThreadPool.Names.GENERIC, false, false, JoinRequest::new,
            (request, channel) -> joinHandler.accept(request, transportJoinCallback(request, channel)));

        transportService.registerRequestHandler(START_JOIN_ACTION_NAME, Names.GENERIC, false, false,
            StartJoinRequest::new,
            (request, channel) -> {
                final DiscoveryNode destination = request.getSourceNode();
                sendJoinRequest(destination, currentTermSupplier.getAsLong(), Optional.of(joinLeaderInTerm.apply(request)));
                channel.sendResponse(Empty.INSTANCE);
            });

        transportService.registerRequestHandler(
            VALIDATE_JOIN_ACTION_NAME,
            ThreadPool.Names.GENERIC,
            ValidateJoinRequest::new,
            (request, channel) -> {
                final ClusterState localState = currentStateSupplier.get();
                if (localState.metadata().clusterUUIDCommitted() &&
                    localState.metadata().clusterUUID().equals(request.getState().metadata().clusterUUID()) == false) {
                    throw new CoordinationStateRejectedException("join validation on cluster state" +
                        " with a different cluster uuid " + request.getState().metadata().clusterUUID() +
                        " than local cluster uuid " + localState.metadata().clusterUUID() + ", rejecting");
                }
                joinValidators.forEach(action -> action.accept(transportService.getLocalNode(), request.getState()));
                channel.sendResponse(Empty.INSTANCE);
            });
    }

    private ActionListener<Void> transportJoinCallback(TransportRequest request, TransportChannel channel) {
        return new ActionListener<>() {

            @Override
            public void onResponse(Void response) {
                try {
                    channel.sendResponse(Empty.INSTANCE);
                } catch (IOException e) {
                    onFailure(e);
                }
            }

            @Override
            public void onFailure(Exception e) {
                try {
                    channel.sendResponse(e);
                } catch (Exception inner) {
                    inner.addSuppressed(e);
                    LOGGER.warn("failed to send back failure on join request", inner);
                }
            }

            @Override
            public String toString() {
                return "JoinCallback{request=" + request + "}";
            }
        };
    }

    boolean isJoinPending() {
        return pendingOutgoingJoins.isEmpty() == false;
    }

    // package-private for testing
    static class FailedJoinAttempt {
        private final DiscoveryNode destination;
        private final JoinRequest joinRequest;
        private final TransportException exception;
        private final long timestamp;

        FailedJoinAttempt(DiscoveryNode destination, JoinRequest joinRequest, TransportException exception) {
            this.destination = destination;
            this.joinRequest = joinRequest;
            this.exception = exception;
            this.timestamp = System.nanoTime();
        }

        void logNow() {
            LOGGER.log(
                getLogLevel(exception),
                () -> new ParameterizedMessage("failed to join {} with {}", destination, joinRequest),
                exception
            );
        }

        static Level getLogLevel(TransportException e) {
            Throwable cause = e.unwrapCause();
            if (cause instanceof CoordinationStateRejectedException ||
                cause instanceof FailedToCommitClusterStateException ||
                cause instanceof NotMasterException) {
                return Level.DEBUG;
            }
            return Level.INFO;
        }

        void logWarnWithTimestamp() {
            LOGGER.warn(() -> new ParameterizedMessage("last failed join attempt was {} ago, failed to join {} with {}",
                            TimeValue.timeValueMillis(TimeValue.nsecToMSec(System.nanoTime() - timestamp)),
                            destination,
                            joinRequest),
                    exception);
        }
    }


    void logLastFailedJoinAttempt() {
        FailedJoinAttempt attempt = lastFailedJoinAttempt.get();
        if (attempt != null) {
            attempt.logWarnWithTimestamp();
            lastFailedJoinAttempt.compareAndSet(attempt, null);
        }
    }


    public void sendJoinRequest(DiscoveryNode destination, long term, Optional<Join> optionalJoin) {
        assert destination.isMasterEligibleNode() : "trying to join master-ineligible " + destination;
        final StatusInfo statusInfo = nodeHealthService.getHealth();
        if (statusInfo.status() == UNHEALTHY) {
            LOGGER.debug("dropping join request to [{}]: [{}]", destination, statusInfo.info());
            return;
        }
        final JoinRequest joinRequest = new JoinRequest(transportService.getLocalNode(), term, optionalJoin);
        final Tuple<DiscoveryNode, JoinRequest> dedupKey = new Tuple<>(destination, joinRequest);
        if (pendingOutgoingJoins.add(dedupKey)) {
            LOGGER.debug("attempting to join {} with {}", destination, joinRequest);
            transportService.sendRequest(
                destination,
                JOIN_ACTION_NAME,
                joinRequest,
                new TransportRequestOptions(joinTimeout),
                new TransportResponseHandler<Empty>() {
                    @Override
                    public Empty read(StreamInput in) {
                        return Empty.INSTANCE;
                    }

                    @Override
                    public void handleResponse(Empty response) {
                        pendingOutgoingJoins.remove(dedupKey);
                        LOGGER.debug("successfully joined {} with {}", destination, joinRequest);
                        lastFailedJoinAttempt.set(null);
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        pendingOutgoingJoins.remove(dedupKey);
                        FailedJoinAttempt attempt = new FailedJoinAttempt(destination, joinRequest, exp);
                        attempt.logNow();
                        lastFailedJoinAttempt.set(attempt);
                    }

                    @Override
                    public String executor() {
                        return Names.SAME;
                    }
                });
        } else {
            LOGGER.debug("already attempting to join {} with request {}, not sending request", destination, joinRequest);
        }
    }

    void sendStartJoinRequest(final StartJoinRequest startJoinRequest, final DiscoveryNode destination) {
        assert startJoinRequest.getSourceNode().isMasterEligibleNode()
            : "sending start-join request for master-ineligible " + startJoinRequest.getSourceNode();
        transportService.sendRequest(destination, START_JOIN_ACTION_NAME,
            startJoinRequest, new TransportResponseHandler<Empty>() {
                @Override
                public Empty read(StreamInput in) {
                    return Empty.INSTANCE;
                }

                @Override
                public void handleResponse(Empty response) {
                    LOGGER.debug("successful response to {} from {}", startJoinRequest, destination);
                }

                @Override
                public void handleException(TransportException exp) {
                    LOGGER.debug(new ParameterizedMessage("failure in response to {} from {}", startJoinRequest, destination), exp);
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }
            });
    }

    void sendValidateJoinRequest(DiscoveryNode node, ClusterState state, ActionListener<TransportResponse.Empty> listener) {
        transportService.sendRequest(node, VALIDATE_JOIN_ACTION_NAME,
            new ValidateJoinRequest(state),
            new TransportRequestOptions(joinTimeout),
            new ActionListenerResponseHandler<>(VALIDATE_JOIN_ACTION_NAME, listener, i -> Empty.INSTANCE, ThreadPool.Names.GENERIC));
    }

    static class JoinTaskListener implements ClusterStateTaskListener {
        private final JoinTaskExecutor.Task task;
        private final ActionListener<Void> joinCallback;

        JoinTaskListener(JoinTaskExecutor.Task task, ActionListener<Void> joinCallback) {
            this.task = task;
            this.joinCallback = joinCallback;
        }

        @Override
        public void onFailure(String source, Exception e) {
            joinCallback.onFailure(e);
        }

        @Override
        public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
            joinCallback.onResponse(null);
        }

        @Override
        public String toString() {
            return "JoinTaskListener{task=" + task + "}";
        }
    }

    interface JoinAccumulator {
        void handleJoinRequest(DiscoveryNode sender, ActionListener<Void> joinCallback);

        default void close(Mode newMode) {
        }
    }

    class LeaderJoinAccumulator implements JoinAccumulator {
        @Override
        public void handleJoinRequest(DiscoveryNode sender, ActionListener<Void> joinCallback) {
            final JoinTaskExecutor.Task task = new JoinTaskExecutor.Task(sender, "join existing leader");
            assert joinTaskExecutor != null;
            masterService.submitStateUpdateTask(
                "node-join",
                task,
                ClusterStateTaskConfig.build(Priority.URGENT),
                joinTaskExecutor,
                new JoinTaskListener(task, joinCallback));
        }

        @Override
        public String toString() {
            return "LeaderJoinAccumulator";
        }
    }

    static class InitialJoinAccumulator implements JoinAccumulator {
        @Override
        public void handleJoinRequest(DiscoveryNode sender, ActionListener<Void> joinCallback) {
            assert false : "unexpected join from " + sender + " during initialisation";
            joinCallback.onFailure(new CoordinationStateRejectedException("join target is not initialised yet"));
        }

        @Override
        public String toString() {
            return "InitialJoinAccumulator";
        }
    }

    static class FollowerJoinAccumulator implements JoinAccumulator {
        @Override
        public void handleJoinRequest(DiscoveryNode sender, ActionListener<Void> joinCallback) {
            joinCallback.onFailure(new CoordinationStateRejectedException("join target is a follower"));
        }

        @Override
        public String toString() {
            return "FollowerJoinAccumulator";
        }
    }

    class CandidateJoinAccumulator implements JoinAccumulator {

        private final Map<DiscoveryNode, ActionListener<Void>> joinRequestAccumulator = new HashMap<>();
        boolean closed;

        @Override
        public void handleJoinRequest(DiscoveryNode sender, ActionListener<Void> joinCallback) {
            assert closed == false : "CandidateJoinAccumulator closed";
            ActionListener<Void> prev = joinRequestAccumulator.put(sender, joinCallback);
            if (prev != null) {
                prev.onFailure(new CoordinationStateRejectedException("received a newer join from " + sender));
            }
        }

        @Override
        public void close(Mode newMode) {
            assert closed == false : "CandidateJoinAccumulator closed";
            closed = true;
            if (newMode == Mode.LEADER) {
                final Map<JoinTaskExecutor.Task, ClusterStateTaskListener> pendingAsTasks = new LinkedHashMap<>();
                joinRequestAccumulator.forEach((key, value) -> {
                    final JoinTaskExecutor.Task task = new JoinTaskExecutor.Task(key, "elect leader");
                    pendingAsTasks.put(task, new JoinTaskListener(task, value));
                });

                final String stateUpdateSource = "elected-as-master ([" + pendingAsTasks.size() + "] nodes joined)";

                pendingAsTasks.put(JoinTaskExecutor.newBecomeMasterTask(), (source, e) -> {
                });
                pendingAsTasks.put(JoinTaskExecutor.newFinishElectionTask(), (source, e) -> {
                });
                joinTaskExecutor = joinTaskExecutorGenerator.get();
                masterService.submitStateUpdateTasks(stateUpdateSource, pendingAsTasks, ClusterStateTaskConfig.build(Priority.URGENT),
                    joinTaskExecutor);
            } else {
                assert newMode == Mode.FOLLOWER : newMode;
                joinTaskExecutor = null;
                joinRequestAccumulator.values().forEach(joinCallback -> joinCallback.onFailure(
                    new CoordinationStateRejectedException("became follower")));
            }

            // CandidateJoinAccumulator is only closed when becoming leader or follower, otherwise it accumulates all joins received
            // regardless of term.
        }

        @Override
        public String toString() {
            return "CandidateJoinAccumulator{" + joinRequestAccumulator.keySet() +
                ", closed=" + closed + '}';
        }
    }
}
