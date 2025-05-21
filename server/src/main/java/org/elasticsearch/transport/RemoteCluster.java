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

package org.elasticsearch.transport;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

import io.crate.common.io.IOUtils;
import io.crate.protocols.postgres.PgClient;
import io.crate.protocols.postgres.PgClientFactory;
import io.crate.replication.logical.metadata.ConnectionInfo;
import io.crate.types.DataTypes;


public class RemoteCluster implements Closeable {

    public enum ConnectionStrategy {
        SNIFF,
        PG_TUNNEL;
    }

    public static final Setting<ConnectionStrategy> REMOTE_CONNECTION_MODE = new Setting<>(
        "mode",
        ConnectionStrategy.SNIFF.name(),
        value -> switch (value.toLowerCase(Locale.ENGLISH)) {
            case "sniff" -> ConnectionStrategy.SNIFF;
            case "pg_tunnel" -> ConnectionStrategy.PG_TUNNEL;
            default -> throw new IllegalArgumentException(
                "Invalid connection mode `" + value + "`, supported modes are: `sniff`, `pg_tunnel`");
        },
        DataTypes.STRING,
        Setting.Property.Dynamic
    );

    private final String clusterName;
    private final ConnectionInfo connectionInfo;
    private final TransportService transportService;
    private final ConnectionStrategy connectionStrategy;
    private final ThreadPool threadPool;
    private final Settings settings;
    private final List<Closeable> toClose = new ArrayList<>();
    private final PgClientFactory pgClientFactory;

    private volatile Client client;


    public RemoteCluster(String name,
                         Settings settings,
                         ConnectionInfo connectionInfo,
                         PgClientFactory pgClientFactory,
                         ThreadPool threadPool,
                         TransportService transportService) {
        this.settings = settings;
        this.threadPool = threadPool;
        this.clusterName = name;
        this.connectionInfo = connectionInfo;
        this.pgClientFactory = pgClientFactory;
        this.transportService = transportService;
        this.connectionStrategy = connectionInfo.mode();
    }

    public Client client() {
        return client;
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(toClose);
    }

    public CompletableFuture<Client> connectAndGetClient() {
        synchronized (this) {
            Client currentClient = client;
            if (currentClient != null) {
                return CompletableFuture.completedFuture(currentClient);
            }
        }
        var futureClient = switch (connectionStrategy) {
            case SNIFF -> connectSniff();
            case PG_TUNNEL -> connectPgTunnel();
        };
        return futureClient.whenComplete((c, err) -> {
            client = c;
        });
    }

    private CompletableFuture<Client> connectPgTunnel() {
        try {
            PgClient pgClient = pgClientFactory.createClient(clusterName, connectionInfo);
            toClose.add(pgClient);
            return pgClient.ensureConnected().thenApply(connection -> pgClient);
        } catch (Throwable e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    private CompletableFuture<Client> connectSniff() {
        var sniffClient = new SniffRemoteClient(
            settings,
            connectionInfo,
            clusterName,
            transportService
        );
        toClose.add(sniffClient);
        try {
            return sniffClient.ensureConnected(null).thenApply(ignored -> sniffClient);
        } catch (Throwable e) {
            return CompletableFuture.failedFuture(e);
        }
    }
}
