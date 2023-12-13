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

package org.elasticsearch.transport;

import java.io.Closeable;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.network.CloseableChannel;
import org.elasticsearch.common.util.concurrent.AbstractLifecycleRunnable;
import org.elasticsearch.threadpool.ThreadPool;

import io.crate.common.collections.Sets;
import io.crate.common.unit.TimeValue;
import io.netty.channel.ChannelFuture;

/**
 * Implements the scheduling and sending of keep alive pings. Client channels send keep alive pings to the
 * server and server channels respond. Pings are only sent at the scheduled time if the channel did not send
 * and receive a message since the last ping.
 */
final class TransportKeepAlive implements Closeable {

    static final int PING_DATA_SIZE = -1;

    private static final byte[] PING_MESSAGE = {'E', 'S', -1, -1, -1, -1};

    private final Logger logger = LogManager.getLogger(TransportKeepAlive.class);
    private final CounterMetric successfulPings = new CounterMetric();
    private final CounterMetric failedPings = new CounterMetric();
    private final ConcurrentMap<TimeValue, ScheduledPing> pingIntervals = new ConcurrentHashMap<>();
    private final Lifecycle lifecycle = new Lifecycle();
    private final ThreadPool threadPool;
    private final BiFunction<CloseableChannel, byte[], ChannelFuture> pingSender;

    TransportKeepAlive(ThreadPool threadPool, BiFunction<CloseableChannel, byte[], ChannelFuture> pingSender) {
        this.threadPool = threadPool;
        this.pingSender = pingSender;

        this.lifecycle.moveToStarted();
    }

    void registerNodeConnection(List<CloseableChannel> nodeChannels, ConnectionProfile connectionProfile) {
        TimeValue pingInterval = connectionProfile.getPingInterval();
        if (pingInterval.millis() < 0) {
            return;
        }

        final ScheduledPing scheduledPing = pingIntervals.computeIfAbsent(pingInterval, ScheduledPing::new);
        scheduledPing.ensureStarted();

        for (CloseableChannel channel : nodeChannels) {
            scheduledPing.addChannel(channel);

            channel.addCloseListener(ActionListener.wrap(() -> {
                scheduledPing.removeChannel(channel);
            }));
        }
    }

    /**
     * Called when a keep alive ping is received. If the channel that received the keep alive ping is a
     * server channel, a ping is sent back. If the channel that received the keep alive is a client channel,
     * this method does nothing as the client initiated the ping in the first place.
     *
     * @param channel that received the keep alive ping
     */
    void receiveKeepAlive(CloseableChannel channel) {
        // The client-side initiates pings and the server-side responds. So if this is a client channel, this
        // method is a no-op.
        if (channel.isServerChannel()) {
            sendPing(channel);
        }
    }

    long successfulPingCount() {
        return successfulPings.count();
    }

    long failedPingCount() {
        return failedPings.count();
    }

    private void sendPing(CloseableChannel channel) {
        var future = pingSender.apply(channel, PING_MESSAGE);
        future.addListener(f -> {
            if (f.isSuccess()) {
                successfulPings.inc();
            } else {
                Throwable e = f.cause();
                if (channel.isOpen()) {
                    logger.debug(() -> new ParameterizedMessage("[{}] failed to send transport ping", channel), e);
                    failedPings.inc();
                } else {
                    logger.trace(() -> new ParameterizedMessage("[{}] failed to send transport ping (channel closed)", channel), e);
                }
            }
        });
    }

    @Override
    public void close() {
        synchronized (lifecycle) {
            lifecycle.moveToStopped();
            lifecycle.moveToClosed();
        }
    }

    private class ScheduledPing extends AbstractLifecycleRunnable {

        private final TimeValue pingInterval;

        private final Set<CloseableChannel> channels = Sets.newConcurrentHashSet();

        private final AtomicBoolean isStarted = new AtomicBoolean(false);
        private volatile long lastPingRelativeMillis;

        private ScheduledPing(TimeValue pingInterval) {
            super(lifecycle, logger);
            this.pingInterval = pingInterval;
            this.lastPingRelativeMillis = threadPool.relativeTimeInMillis();
        }

        void ensureStarted() {
            if (isStarted.get() == false && isStarted.compareAndSet(false, true)) {
                threadPool.schedule(this, pingInterval, ThreadPool.Names.GENERIC);
            }
        }

        void addChannel(CloseableChannel channel) {
            channels.add(channel);
        }

        void removeChannel(CloseableChannel channel) {
            channels.remove(channel);
        }

        @Override
        protected void doRunInLifecycle() {
            for (var channel : channels) {
                // In the future it is possible that we may want to kill a channel if we have not read from
                // the channel since the last ping. However, this will need to be backwards compatible with
                // pre-6.6 nodes that DO NOT respond to pings
                if (needsKeepAlivePing(channel)) {
                    sendPing(channel);
                }
            }
            this.lastPingRelativeMillis = threadPool.relativeTimeInMillis();
        }

        @Override
        protected void onAfterInLifecycle() {
            threadPool.scheduleUnlessShuttingDown(pingInterval, ThreadPool.Names.GENERIC, this);
        }

        @Override
        public void onFailure(Exception e) {
            logger.warn("failed to send ping transport message", e);
        }

        private boolean needsKeepAlivePing(CloseableChannel channel) {
            long accessedDelta = channel.lastAccessedTime() - lastPingRelativeMillis;
            return accessedDelta <= 0;
        }
    }
}
