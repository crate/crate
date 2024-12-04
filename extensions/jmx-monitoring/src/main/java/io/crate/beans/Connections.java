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

import java.util.function.Supplier;

import io.crate.protocols.ConnectionStats;

public final class Connections implements ConnectionsMBean {

    public static final String NAME = "io.crate.monitoring:type=Connections";
    private final Supplier<ConnectionStats> httpStats;
    private final Supplier<ConnectionStats> psqlStats;
    private final Supplier<ConnectionStats> transportStats;

    public Connections(Supplier<ConnectionStats> httpStats,
                       Supplier<ConnectionStats> psqlStats,
                       Supplier<ConnectionStats> transportStats) {
        this.httpStats = httpStats;
        this.psqlStats = psqlStats;
        this.transportStats = transportStats;
    }


    @Override
    public long getHttpOpen() {
        return httpStats.get().open();
    }

    @Override
    public long getHttpTotal() {
        return httpStats.get().total();
    }

    @Override
    public long getHttpMessagesReceived() {
        return httpStats.get().receivedMsgs();
    }

    @Override
    public long getHttpBytesReceived() {
        return httpStats.get().receivedBytes();
    }

    @Override
    public long getHttpMessagesSent() {
        return httpStats.get().sentMsgs();
    }

    @Override
    public long getHttpBytesSent() {
        return httpStats.get().sentBytes();
    }

    @Override
    public long getPsqlOpen() {
        return psqlStats.get().open();
    }

    @Override
    public long getPsqlTotal() {
        return psqlStats.get().total();
    }

    @Override
    public long getPsqlMessagesReceived() {
        return psqlStats.get().receivedMsgs();
    }

    @Override
    public long getPsqlBytesReceived() {
        return psqlStats.get().receivedBytes();
    }

    @Override
    public long getPsqlMessagesSent() {
        return psqlStats.get().sentMsgs();
    }

    @Override
    public long getPsqlBytesSent() {
        return psqlStats.get().sentBytes();
    }

    @Override
    public long getTransportOpen() {
        return transportStats.get().open();
    }

    @Override
    public long getTransportTotal() {
        return transportStats.get().total();
    }

    @Override
    public long getTransportMessagesReceived() {
        return transportStats.get().receivedMsgs();
    }

    @Override
    public long getTransportBytesReceived() {
        return transportStats.get().receivedBytes();
    }

    @Override
    public long getTransportMessagesSent() {
        return transportStats.get().sentMsgs();
    }

    @Override
    public long getTransportBytesSent() {
        return transportStats.get().sentBytes();
    }
}
