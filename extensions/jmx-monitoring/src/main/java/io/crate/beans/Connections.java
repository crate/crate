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

import io.crate.protocols.ConnectionStats;
import org.elasticsearch.http.HttpStats;

import java.util.function.LongSupplier;
import java.util.function.Supplier;

public final class Connections implements ConnectionsMBean {

    public static final String NAME = "io.crate.monitoring:type=Connections";
    private final Supplier<HttpStats> httpStats;
    private final Supplier<ConnectionStats> psqlStats;
    private final LongSupplier transportConnections;

    public Connections(Supplier<HttpStats> httpStats,
                       Supplier<ConnectionStats> psqlStats,
                       LongSupplier transportConnections) {
        this.httpStats = httpStats;
        this.psqlStats = psqlStats;
        this.transportConnections = transportConnections;
    }


    @Override
    public long getHttpOpen() {
        HttpStats httpStats = this.httpStats.get();
        return httpStats == null ? 0L : httpStats.getServerOpen();
    }

    @Override
    public long getHttpTotal() {
        HttpStats httpStats = this.httpStats.get();
        return httpStats == null ? 0L : httpStats.getTotalOpen();
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
    public long getTransportOpen() {
        return transportConnections.getAsLong();
    }
}
