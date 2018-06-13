/*
 * This file is part of a module with proprietary Enterprise Features.
 *
 * Licensed to Crate.io Inc. ("Crate.io") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 *
 * To use this file, Crate.io must have given you permission to enable and
 * use such Enterprise Features and you must have a valid Enterprise or
 * Subscription Agreement with Crate.io.  If you enable or use the Enterprise
 * Features, you represent and warrant that you have a valid Enterprise or
 * Subscription Agreement with Crate.io.  Your use of the Enterprise Features
 * if governed by the terms and conditions of your Enterprise or Subscription
 * Agreement with Crate.io.
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
