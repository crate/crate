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

package io.crate.beans;

import org.elasticsearch.transport.StatsTracker;

public final class Connections implements ConnectionsMBean {

    public static final String NAME = "io.crate.monitoring:type=Connections";
    private final StatsTracker httpStats;
    private final StatsTracker psqlStats;
    private final StatsTracker transportStats;

    public Connections(StatsTracker httpStats,
                       StatsTracker psqlStats,
                       StatsTracker transportStats) {
        this.httpStats = httpStats;
        this.psqlStats = psqlStats;
        this.transportStats = transportStats;
    }


    @Override
    public long getHttpOpen() {
        return httpStats.openConnections();
    }

    @Override
    public long getHttpTotal() {
        return httpStats.totalConnections();
    }

    @Override
    public long getHttpMessagesReceived() {
        return httpStats.messagesReceived();
    }

    @Override
    public long getHttpBytesReceived() {
        return httpStats.bytesReceived();
    }

    @Override
    public long getHttpMessagesSent() {
        return httpStats.messagesSent();
    }

    @Override
    public long getHttpBytesSent() {
        return httpStats.bytesSent();
    }

    @Override
    public long getPsqlOpen() {
        return psqlStats.openConnections();
    }

    @Override
    public long getPsqlTotal() {
        return psqlStats.totalConnections();
    }

    @Override
    public long getPsqlMessagesReceived() {
        return psqlStats.messagesReceived();
    }

    @Override
    public long getPsqlBytesReceived() {
        return psqlStats.bytesReceived();
    }

    @Override
    public long getPsqlMessagesSent() {
        return psqlStats.messagesSent();
    }

    @Override
    public long getPsqlBytesSent() {
        return psqlStats.bytesSent();
    }

    @Override
    public long getTransportOpen() {
        return transportStats.openConnections();
    }

    @Override
    public long getTransportTotal() {
        return transportStats.totalConnections();
    }

    @Override
    public long getTransportMessagesReceived() {
        return transportStats.messagesReceived();
    }

    @Override
    public long getTransportBytesReceived() {
        return transportStats.bytesReceived();
    }

    @Override
    public long getTransportMessagesSent() {
        return transportStats.messagesSent();
    }

    @Override
    public long getTransportBytesSent() {
        return transportStats.bytesSent();
    }
}
