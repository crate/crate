/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.client;

import io.crate.action.sql.SQLBulkRequest;
import io.crate.action.sql.SQLBulkResponse;
import io.crate.action.sql.SQLRequest;
import io.crate.action.sql.SQLResponse;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterNameModule;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.indices.breaker.CircuitBreakerModule;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPoolModule;
import org.elasticsearch.transport.TransportModule;
import org.elasticsearch.transport.TransportService;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.settings.Settings.settingsBuilder;

public class CrateClient {

    private final Settings settings;
    private final InternalCrateClient internalClient;
    private final TransportService transportService;
    private final ThreadPool threadPool;

    private static final ESLogger logger = Loggers.getLogger(CrateClient.class);

    public CrateClient(Settings pSettings, String ... servers) throws
            ElasticsearchException {

        Settings.Builder builder = settingsBuilder()
                .put(pSettings)
                .put("network.server", false)
                .put("node.client", true)
                .put("client.transport.ignore_cluster_name", true)
                .put("node.name", "crate-client-" + UUID.randomUUID().toString())
                // Client uses only the SAME or GENERIC thread-pool (see TransportNodeActionProxy)
                // so the other thread-pools can be limited to 1 thread to not waste resources
                .put("threadpool.search.size", 1)
                .put("threadpool.index.size", 1)
                .put("threadpool.bulk.size", 1)
                .put("threadpool.get.size", 1)
                .put("threadpool.percolate.size", 1);

        if (builder.get("name") == null){
            builder.put("name", "crate_client");
        }

        this.settings = builder.build();

        threadPool = new ThreadPool(this.settings);
        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry();

        ModulesBuilder modules = new ModulesBuilder();
        modules.add(new CrateClientModule());
        modules.add(new Version.Module(Version.CURRENT));
        modules.add(new ThreadPoolModule(threadPool));

        modules.add(new SettingsModule(this.settings));

        modules.add(new ClusterNameModule(this.settings));
        modules.add(new TransportModule(this.settings, namedWriteableRegistry));
        modules.add(new CircuitBreakerModule(this.settings));

        Injector injector = modules.createInjector();
        transportService = injector.getInstance(TransportService.class).start();
        internalClient = injector.getInstance(InternalCrateClient.class);

        for (String server : servers) {
            TransportAddress transportAddress = tryCreateTransportFor(server);
            if(transportAddress != null) {
                internalClient.addTransportAddress(transportAddress);
            }
        }
    }

    public CrateClient() {
        this(Settings.Builder.EMPTY_SETTINGS);
    }

    public CrateClient(String... servers) {
        this(Settings.Builder.EMPTY_SETTINGS, servers);
    }

    @Nullable
    protected TransportAddress tryCreateTransportFor(String server) {
        URI uri;
        try {
            uri = new URI(server.contains("://") ? server : "tcp://" + server);
        } catch (URISyntaxException e) {
            logger.warn("Malformed URI syntax: {}", e, server);
            return null;
        }

        if (uri.getHost() != null) {
            return new InetSocketTransportAddress(
                    new InetSocketAddress(uri.getHost(), uri.getPort() > -1 ? uri.getPort() : 4300));
        }
        return null;
    }

    public ActionFuture<SQLResponse> sql(String stmt) {
        return sql(new SQLRequest(stmt));
    }

    public ActionFuture<SQLResponse> sql(SQLRequest request) {
        return internalClient.sql(request);
    }

    public void sql(String stmt, ActionListener<SQLResponse> listener) {
        sql(new SQLRequest(stmt), listener);
    }

    public void sql(SQLRequest request, ActionListener<SQLResponse> listener) {
        internalClient.sql(request, listener);
    }

    public ActionFuture<SQLBulkResponse> bulkSql(SQLBulkRequest bulkRequest) {
        return internalClient.bulkSql(bulkRequest);
    }

    public void bulkSql(SQLBulkRequest bulkRequest, ActionListener<SQLBulkResponse> listener) {
        internalClient.bulkSql(bulkRequest, listener);
    }

    public Settings settings() {
        return settings;
    }

    public void close() {
        transportService.stop();
        internalClient.close();
        threadPool.shutdown();
        try {
            threadPool.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().isInterrupted();
        }
    }
}
