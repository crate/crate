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

package org.elasticsearch.cluster.service;

import static org.elasticsearch.common.util.concurrent.EsExecutors.daemonThreadFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.jetbrains.annotations.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.ClusterStateTaskConfig;
import org.elasticsearch.cluster.LocalNodeMasterListener;
import org.elasticsearch.cluster.NodeConnectionsService;
import org.elasticsearch.cluster.TimeoutClusterStateListener;
import org.elasticsearch.cluster.metadata.ProcessClusterEventTimeoutException;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.PrioritizedEsThreadPoolExecutor;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import io.crate.common.unit.TimeValue;

public class ClusterApplierService extends AbstractLifecycleComponent implements ClusterApplier {

    private static final Logger LOGGER = LogManager.getLogger(ClusterApplierService.class);

    public static final Setting<TimeValue> CLUSTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING =
        Setting.positiveTimeSetting("cluster.service.slow_task_logging_threshold", TimeValue.timeValueSeconds(30),
            Setting.Property.Dynamic, Setting.Property.NodeScope);

    public static final String CLUSTER_UPDATE_THREAD_NAME = "clusterApplierService#updateTask";

    private final ClusterSettings clusterSettings;
    protected final ThreadPool threadPool;

    private volatile TimeValue slowTaskLoggingThreshold;

    private volatile PrioritizedEsThreadPoolExecutor threadPoolExecutor;

    /**
     * Those 3 state listeners are changing infrequently - CopyOnWriteArrayList is just fine
     */
    private final Collection<ClusterStateApplier> highPriorityStateAppliers = new CopyOnWriteArrayList<>();
    private final Collection<ClusterStateApplier> normalPriorityStateAppliers = new CopyOnWriteArrayList<>();
    private final Collection<ClusterStateApplier> lowPriorityStateAppliers = new CopyOnWriteArrayList<>();

    private final Collection<ClusterStateListener> clusterStateListeners = new CopyOnWriteArrayList<>();
    private final Map<TimeoutClusterStateListener, NotifyTimeout> timeoutClusterStateListeners = new ConcurrentHashMap<>();

    private final AtomicReference<ClusterState> state; // last applied state

    private final String nodeName;

    private NodeConnectionsService nodeConnectionsService;

    public ClusterApplierService(String nodeName, Settings settings, ClusterSettings clusterSettings, ThreadPool threadPool) {
        this.clusterSettings = clusterSettings;
        this.threadPool = threadPool;
        this.state = new AtomicReference<>();
        this.nodeName = nodeName;

        this.slowTaskLoggingThreshold = CLUSTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING.get(settings);
        this.clusterSettings.addSettingsUpdateConsumer(CLUSTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING,
            this::setSlowTaskLoggingThreshold);
    }

    private void setSlowTaskLoggingThreshold(TimeValue slowTaskLoggingThreshold) {
        this.slowTaskLoggingThreshold = slowTaskLoggingThreshold;
    }

    public synchronized void setNodeConnectionsService(NodeConnectionsService nodeConnectionsService) {
        assert this.nodeConnectionsService == null : "nodeConnectionsService is already set";
        this.nodeConnectionsService = nodeConnectionsService;
    }

    @Override
    public void setInitialState(ClusterState initialState) {
        if (lifecycle.started()) {
            throw new IllegalStateException("can't set initial state when started");
        }
        assert state.get() == null : "state is already set";
        state.set(initialState);
    }

    @Override
    protected synchronized void doStart() {
        Objects.requireNonNull(nodeConnectionsService, "please set the node connection service before starting");
        Objects.requireNonNull(state.get(), "please set initial state before starting");
        threadPoolExecutor = createThreadPoolExecutor();
    }

    protected PrioritizedEsThreadPoolExecutor createThreadPoolExecutor() {
        return EsExecutors.newSinglePrioritizing(
            nodeName + "/" + CLUSTER_UPDATE_THREAD_NAME,
            daemonThreadFactory(nodeName, CLUSTER_UPDATE_THREAD_NAME),
            threadPool.scheduler());
    }

    class UpdateTask extends SourcePrioritizedRunnable implements Function<ClusterState, ClusterState> {
        final ClusterApplyListener listener;
        final Function<ClusterState, ClusterState> updateFunction;

        UpdateTask(Priority priority, String source, ClusterApplyListener listener,
                   Function<ClusterState, ClusterState> updateFunction) {
            super(priority, source);
            this.listener = listener;
            this.updateFunction = updateFunction;
        }

        @Override
        public ClusterState apply(ClusterState clusterState) {
            return updateFunction.apply(clusterState);
        }

        @Override
        public void run() {
            runTask(this);
        }
    }

    @Override
    protected synchronized void doStop() {
        for (Map.Entry<TimeoutClusterStateListener, NotifyTimeout> onGoingTimeout : timeoutClusterStateListeners.entrySet()) {
            try {
                onGoingTimeout.getValue().cancel();
                onGoingTimeout.getKey().onClose();
            } catch (Exception ex) {
                LOGGER.debug("failed to notify listeners on shutdown", ex);
            }
        }
        ThreadPool.terminate(threadPoolExecutor, 10, TimeUnit.SECONDS);
    }

    @Override
    protected synchronized void doClose() {
    }


    public ThreadPool threadPool() {
        return threadPool;
    }

    /**
     * The current cluster state.
     * Should be renamed to appliedClusterState
     */
    public ClusterState state() {
        assert assertNotCalledFromClusterStateApplier("the applied cluster state is not yet available");
        ClusterState clusterState = this.state.get();
        assert clusterState != null : "initial cluster state not set yet";
        return clusterState;
    }

    /**
     * Adds a high priority applier of updated cluster states.
     */
    public void addHighPriorityApplier(ClusterStateApplier applier) {
        highPriorityStateAppliers.add(applier);
    }

    /**
     * Adds an applier which will be called after all high priority and normal appliers have been called.
     */
    public void addLowPriorityApplier(ClusterStateApplier applier) {
        lowPriorityStateAppliers.add(applier);
    }

    /**
     * Adds a applier of updated cluster states.
     */
    public void addStateApplier(ClusterStateApplier applier) {
        normalPriorityStateAppliers.add(applier);
    }

    /**
     * Removes an applier of updated cluster states.
     */
    public void removeApplier(ClusterStateApplier applier) {
        normalPriorityStateAppliers.remove(applier);
        highPriorityStateAppliers.remove(applier);
        lowPriorityStateAppliers.remove(applier);
    }

    /**
     * Add a listener for updated cluster states
     */
    public void addListener(ClusterStateListener listener) {
        clusterStateListeners.add(listener);
    }

    /**
     * Removes a listener for updated cluster states.
     */
    public void removeListener(ClusterStateListener listener) {
        clusterStateListeners.remove(listener);
    }

    /**
     * Removes a timeout listener for updated cluster states.
     */
    public void removeTimeoutListener(TimeoutClusterStateListener listener) {
        final NotifyTimeout timeout = timeoutClusterStateListeners.remove(listener);
        if (timeout != null) {
            timeout.cancel();
        }
    }

    /**
     * Add a listener for on/off local node master events
     */
    public void addLocalNodeMasterListener(LocalNodeMasterListener listener) {
        addListener(listener);
    }

    /**
     * Adds a cluster state listener that is expected to be removed during a short period of time.
     * If provided, the listener will be notified once a specific time has elapsed.
     *
     * NOTE: the listener is not removed on timeout. This is the responsibility of the caller.
     */
    public void addTimeoutListener(@Nullable final TimeValue timeout, final TimeoutClusterStateListener listener) {
        if (lifecycle.stoppedOrClosed()) {
            listener.onClose();
            return;
        }
        // call the post added notification on the same event thread
        try {
            threadPoolExecutor.execute(new SourcePrioritizedRunnable(Priority.HIGH, "_add_listener_") {
                @Override
                public void run() {
                    final NotifyTimeout notifyTimeout = new NotifyTimeout(listener, timeout);
                    final NotifyTimeout previous = timeoutClusterStateListeners.put(listener, notifyTimeout);
                    assert previous == null : "Added same listener [" + listener + "]";
                    if (lifecycle.stoppedOrClosed()) {
                        listener.onClose();
                        return;
                    }
                    if (timeout != null) {
                        notifyTimeout.cancellable = threadPool.schedule(notifyTimeout, timeout, ThreadPool.Names.GENERIC);
                    }
                    listener.postAdded();
                }
            });
        } catch (EsRejectedExecutionException e) {
            if (lifecycle.stoppedOrClosed()) {
                listener.onClose();
            } else {
                throw e;
            }
        }
    }

    public void runOnApplierThread(final String source, Consumer<ClusterState> clusterStateConsumer,
                                   final ClusterApplyListener listener, Priority priority) {
        submitStateUpdateTask(source, ClusterStateTaskConfig.build(priority),
            (clusterState) -> {
                clusterStateConsumer.accept(clusterState);
                return clusterState;
            },
            listener);
    }

    public void runOnApplierThread(final String source, Consumer<ClusterState> clusterStateConsumer,
                                   final ClusterApplyListener listener) {
        runOnApplierThread(source, clusterStateConsumer, listener, Priority.HIGH);
    }

    @Override
    public void onNewClusterState(final String source, final Supplier<ClusterState> clusterStateSupplier,
                                  final ClusterApplyListener listener) {
        Function<ClusterState, ClusterState> applyFunction = currentState -> {
            ClusterState nextState = clusterStateSupplier.get();
            if (nextState != null) {
                return nextState;
            } else {
                return currentState;
            }
        };
        submitStateUpdateTask(source, ClusterStateTaskConfig.build(Priority.HIGH), applyFunction, listener);
    }

    private void submitStateUpdateTask(final String source, final ClusterStateTaskConfig config,
                                       final Function<ClusterState, ClusterState> executor,
                                       final ClusterApplyListener listener) {
        if (!lifecycle.started()) {
            return;
        }
        try {
            UpdateTask updateTask = new UpdateTask(config.priority(), source, new SafeClusterApplyListener(listener, LOGGER), executor);
            if (config.timeout() != null) {
                threadPoolExecutor.execute(updateTask, config.timeout(),
                    () -> threadPool.generic().execute(
                        () -> listener.onFailure(source, new ProcessClusterEventTimeoutException(config.timeout(), source))));
            } else {
                threadPoolExecutor.execute(updateTask);
            }
        } catch (EsRejectedExecutionException e) {
            // ignore cases where we are shutting down..., there is really nothing interesting
            // to be done here...
            if (!lifecycle.stoppedOrClosed()) {
                throw e;
            }
        }
    }

    /** asserts that the current thread is <b>NOT</b> the cluster state update thread */
    public static boolean assertNotClusterStateUpdateThread(String reason) {
        assert Thread.currentThread().getName().contains(CLUSTER_UPDATE_THREAD_NAME) == false :
            "Expected current thread [" + Thread.currentThread() + "] to not be the cluster state update thread. Reason: [" + reason + "]";
        return true;
    }

    /** asserts that the current stack trace does <b>NOT</b> involve a cluster state applier */
    private static boolean assertNotCalledFromClusterStateApplier(String reason) {
        if (Thread.currentThread().getName().contains(CLUSTER_UPDATE_THREAD_NAME)) {
            for (StackTraceElement element : Thread.currentThread().getStackTrace()) {
                final String className = element.getClassName();
                final String methodName = element.getMethodName();
                if (className.equals(ClusterStateObserver.class.getName())) {
                    // people may start an observer from an applier
                    return true;
                } else if (className.equals(ClusterApplierService.class.getName())
                    && methodName.equals("callClusterStateAppliers")) {
                    throw new AssertionError("should not be called by a cluster state applier. reason [" + reason + "]");
                }
            }
        }
        return true;
    }

    private void runTask(UpdateTask task) {
        if (!lifecycle.started()) {
            LOGGER.debug("processing [{}]: ignoring, cluster applier service not started", task.source);
            return;
        }

        LOGGER.debug("processing [{}]: execute", task.source);
        final ClusterState previousClusterState = state.get();

        long startTimeMS = currentTimeInMillis();
        final StopWatch stopWatch = new StopWatch();
        final ClusterState newClusterState;
        try {
            try (Releasable ignored = stopWatch.timing("running task [" + task.source + ']')) {
                newClusterState = task.apply(previousClusterState);
            }
        } catch (Exception e) {
            TimeValue executionTime = TimeValue.timeValueMillis(Math.max(0, currentTimeInMillis() - startTimeMS));
            LOGGER.trace(() -> new ParameterizedMessage(
                "failed to execute cluster state applier in [{}], state:\nversion [{}], source [{}]\n{}",
                executionTime, previousClusterState.version(), task.source, previousClusterState), e);
            warnAboutSlowTaskIfNeeded(executionTime, task.source, stopWatch);
            task.listener.onFailure(task.source, e);
            return;
        }

        if (previousClusterState == newClusterState) {
            TimeValue executionTime = TimeValue.timeValueMillis(Math.max(0, currentTimeInMillis() - startTimeMS));
            LOGGER.debug("processing [{}]: took [{}] no change in cluster state", task.source, executionTime);
            warnAboutSlowTaskIfNeeded(executionTime, task.source, stopWatch);
            task.listener.onSuccess(task.source);
        } else {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.debug("cluster state updated, version [{}], source [{}]\n{}", newClusterState.version(), task.source,
                    newClusterState);
            } else {
                LOGGER.debug("cluster state updated, version [{}], source [{}]", newClusterState.version(), task.source);
            }
            try {
                applyChanges(task, previousClusterState, newClusterState, stopWatch);
                TimeValue executionTime = TimeValue.timeValueMillis(Math.max(0, currentTimeInMillis() - startTimeMS));
                LOGGER.debug("processing [{}]: took [{}] done applying updated cluster state (version: {}, uuid: {})", task.source,
                    executionTime, newClusterState.version(),
                    newClusterState.stateUUID());
                warnAboutSlowTaskIfNeeded(executionTime, task.source, stopWatch);
                task.listener.onSuccess(task.source);
            } catch (Exception e) {
                TimeValue executionTime = TimeValue.timeValueMillis(Math.max(0, currentTimeInMillis() - startTimeMS));
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.warn(new ParameterizedMessage(
                            "failed to apply updated cluster state in [{}]:\nversion [{}], uuid [{}], source [{}]\n{}",
                            executionTime, newClusterState.version(), newClusterState.stateUUID(), task.source, newClusterState), e);
                } else {
                    LOGGER.warn(new ParameterizedMessage(
                            "failed to apply updated cluster state in [{}]:\nversion [{}], uuid [{}], source [{}]",
                            executionTime, newClusterState.version(), newClusterState.stateUUID(), task.source), e);
                }
                // failing to apply a cluster state with an exception indicates a bug in validation or in one of the appliers; if we
                // continue we will retry with the same cluster state but that might not help.
                assert applicationMayFail();
                task.listener.onFailure(task.source, e);
            }
        }
    }

    private void applyChanges(UpdateTask task, ClusterState previousClusterState, ClusterState newClusterState, StopWatch stopWatch) {
        ClusterChangedEvent clusterChangedEvent = new ClusterChangedEvent(task.source, newClusterState, previousClusterState);
        // new cluster state, notify all listeners
        final DiscoveryNodes.Delta nodesDelta = clusterChangedEvent.nodesDelta();
        if (nodesDelta.hasChanges() && LOGGER.isInfoEnabled()) {
            String summary = nodesDelta.shortSummary();
            if (summary.length() > 0) {
                LOGGER.info("{}, term: {}, version: {}, reason: {}",
                    summary, newClusterState.term(), newClusterState.version(), task.source);
            }
        }

        LOGGER.trace("connecting to nodes of cluster state with version {}", newClusterState.version());
        try (Releasable ignored = stopWatch.timing("connecting to new nodes")) {
            connectToNodesAndWait(newClusterState);
        }

        // nothing to do until we actually recover from the gateway or any other block indicates we need to disable persistency
        if (clusterChangedEvent.state().blocks().disableStatePersistence() == false && clusterChangedEvent.metadataChanged()) {
            LOGGER.debug("applying settings from cluster state with version {}", newClusterState.version());
            final Settings incomingSettings = clusterChangedEvent.state().metadata().settings();
            try (Releasable ignored = stopWatch.timing("applying settings")) {
                clusterSettings.applySettings(incomingSettings);
            }
        }

        LOGGER.debug("apply cluster state with version {}", newClusterState.version());
        callClusterStateAppliers(clusterChangedEvent, stopWatch);

        nodeConnectionsService.disconnectFromNodesExcept(newClusterState.nodes());

        LOGGER.debug("set locally applied cluster state to version {}", newClusterState.version());
        state.set(newClusterState);

        callClusterStateListeners(clusterChangedEvent, stopWatch);
    }

    protected void connectToNodesAndWait(ClusterState newClusterState) {
        // can't wait for an ActionFuture on the cluster applier thread, but we do want to block the thread here, so use a CountDownLatch.
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        nodeConnectionsService.connectToNodes(newClusterState.nodes(), countDownLatch::countDown);
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            LOGGER.debug("interrupted while connecting to nodes, continuing", e);
            Thread.currentThread().interrupt();
        }
    }

    private void callClusterStateAppliers(ClusterChangedEvent clusterChangedEvent, StopWatch stopWatch) {
        callClusterStateAppliers(clusterChangedEvent, stopWatch, highPriorityStateAppliers);
        callClusterStateAppliers(clusterChangedEvent, stopWatch, normalPriorityStateAppliers);
        callClusterStateAppliers(clusterChangedEvent, stopWatch, lowPriorityStateAppliers);
    }

    private static void callClusterStateAppliers(ClusterChangedEvent clusterChangedEvent, StopWatch stopWatch,
                                                 Collection<ClusterStateApplier> clusterStateAppliers) {
        for (ClusterStateApplier applier : clusterStateAppliers) {
            LOGGER.trace("calling [{}] with change to version [{}]", applier, clusterChangedEvent.state().version());
            try (Releasable ignored = stopWatch.timing("running applier [" + applier + "]")) {
                applier.applyClusterState(clusterChangedEvent);
            }
        }
    }

    private void callClusterStateListeners(ClusterChangedEvent clusterChangedEvent, StopWatch stopWatch) {
        callClusterStateListener(clusterChangedEvent, stopWatch, clusterStateListeners);
        callClusterStateListener(clusterChangedEvent, stopWatch, timeoutClusterStateListeners.keySet());
    }

    private void callClusterStateListener(ClusterChangedEvent clusterChangedEvent, StopWatch stopWatch,
                                          Collection<? extends ClusterStateListener> listeners) {
        for (ClusterStateListener listener : listeners) {
            try {
                LOGGER.trace("calling [{}] with change to version [{}]", listener, clusterChangedEvent.state().version());
                try (Releasable ignored = stopWatch.timing("notifying listener [" + listener + "]")) {
                    listener.clusterChanged(clusterChangedEvent);
                }
            } catch (Exception ex) {
                LOGGER.warn("failed to notify ClusterStateListener", ex);
            }
        }
    }

    private static class SafeClusterApplyListener implements ClusterApplyListener {
        private final ClusterApplyListener listener;
        private final Logger logger;

        SafeClusterApplyListener(ClusterApplyListener listener, Logger logger) {
            this.listener = listener;
            this.logger = logger;
        }

        @Override
        public void onFailure(String source, Exception e) {
            try {
                listener.onFailure(source, e);
            } catch (Exception inner) {
                inner.addSuppressed(e);
                logger.error(new ParameterizedMessage(
                        "exception thrown by listener notifying of failure from [{}]", source), inner);
            }
        }

        @Override
        public void onSuccess(String source) {
            try {
                listener.onSuccess(source);
            } catch (Exception e) {
                logger.error(new ParameterizedMessage(
                    "exception thrown by listener while notifying of cluster state processed from [{}]", source), e);
            }
        }
    }

    private void warnAboutSlowTaskIfNeeded(TimeValue executionTime, String source, StopWatch stopWatch) {
        if (executionTime.millis() > slowTaskLoggingThreshold.millis()) {
            LOGGER.warn("cluster state applier task [{}] took [{}] which is above the warn threshold of [{}]: {}", source, executionTime,
                slowTaskLoggingThreshold, Arrays.stream(stopWatch.taskInfo())
                    .map(ti -> '[' + ti.getTaskName() + "] took [" + ti.getTime().millis() + "ms]").collect(Collectors.joining(", ")));
        }
    }

    private class NotifyTimeout implements Runnable {
        final TimeoutClusterStateListener listener;
        @Nullable
        final TimeValue timeout;
        volatile Scheduler.Cancellable cancellable;

        NotifyTimeout(TimeoutClusterStateListener listener, @Nullable TimeValue timeout) {
            this.listener = listener;
            this.timeout = timeout;
        }

        public void cancel() {
            if (cancellable != null) {
                cancellable.cancel();
            }
        }

        @Override
        public void run() {
            assert timeout != null : "This should only ever execute if there's an actual timeout set";
            if (cancellable != null && cancellable.isCancelled()) {
                return;
            }
            if (lifecycle.stoppedOrClosed()) {
                listener.onClose();
            } else {
                listener.onTimeout(this.timeout);
            }
            // note, we rely on the listener to remove itself in case of timeout if needed
        }
    }

    // this one is overridden in tests so we can control time
    protected long currentTimeInMillis() {
        return threadPool.relativeTimeInMillis();
    }

    // overridden by tests that need to check behaviour in the event of an application failure
    protected boolean applicationMayFail() {
        return false;
    }
}
