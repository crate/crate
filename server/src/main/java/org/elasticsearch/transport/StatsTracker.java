/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.transport;

import java.util.concurrent.atomic.LongAdder;

import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.metrics.MeanMetric;

import io.crate.protocols.ConnectionStats;

public class StatsTracker {

    private final CounterMetric openChannelsMetric = new CounterMetric();
    private final CounterMetric totalChannelsMetric = new CounterMetric();

    private final LongAdder bytesReceivedMetric = new LongAdder();
    /**
     * Use an additional counter for messages received as one payload (which increases received bytes)
     * can hold multiple messages/fragments. See {@link org.elasticsearch.transport.InboundPipeline}
     */
    private final CounterMetric messagesReceivedMetric = new CounterMetric();
    private final MeanMetric bytesSentMetric = new MeanMetric();

    public void incrementOpenChannels() {
        openChannelsMetric.inc();
        totalChannelsMetric.inc();
    }

    public void decrementOpenChannels() {
        openChannelsMetric.dec();
    }

    public void incrementBytesReceived(long bytesReceived) {
        this.bytesReceivedMetric.add(bytesReceived);
    }

    public void incrementMessagesReceived() {
        messagesReceivedMetric.inc();
    }

    public void incrementBytesSent(long bytesSent) {
        bytesSentMetric.inc(bytesSent);
    }

    public long openConnections() {
        return openChannelsMetric.count();
    }

    public long totalConnections() {
        return totalChannelsMetric.count();
    }

    public long messagesReceived() {
        return messagesReceivedMetric.count();
    }

    public long bytesReceived() {
        return bytesReceivedMetric.sum();
    }

    public long bytesSent() {
        return bytesSentMetric.sum();
    }

    public long messagesSent() {
        return bytesSentMetric.count();
    }

    public ConnectionStats stats() {
        return new ConnectionStats(
            openConnections(),
            totalConnections(),
            messagesReceived(),
            bytesReceived(),
            messagesSent(),
            bytesSent());
    }
}
