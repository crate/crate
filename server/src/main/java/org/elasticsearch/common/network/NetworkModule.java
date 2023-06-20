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

package org.elasticsearch.common.network;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.routing.allocation.command.AllocateEmptyPrimaryAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.AllocateReplicaAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.AllocateStalePrimaryAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.CancelAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.plugins.NetworkPlugin;
import org.elasticsearch.threadpool.ThreadPool;

import io.crate.auth.Authentication;
import io.crate.common.CheckedFunction;
import io.crate.netty.NettyBootstrap;
import io.crate.protocols.ssl.SslContextProvider;

/**
 * A module to handle registering and binding all network related classes.
 */
public final class NetworkModule {

    public static final String HTTP_TYPE_KEY = "http.type";
    public static final String HTTP_TYPE_DEFAULT_KEY = "http.type.default";
    public static final String TRANSPORT_TYPE_DEFAULT_KEY = "transport.type.default";

    public static final Setting<String> HTTP_DEFAULT_TYPE_SETTING = Setting.simpleString(HTTP_TYPE_DEFAULT_KEY, Property.NodeScope);
    public static final Setting<String> HTTP_TYPE_SETTING = Setting.simpleString(HTTP_TYPE_KEY, Property.NodeScope);

    private final Settings settings;

    private static final List<NamedWriteableRegistry.Entry> NAMED_WRITEABLES = new ArrayList<>();
    private static final List<NamedXContentRegistry.Entry> NAMED_XCONTENTS = new ArrayList<>();

    static {
        registerAllocationCommand(CancelAllocationCommand::new, CancelAllocationCommand::fromXContent,
            CancelAllocationCommand.COMMAND_NAME_FIELD);
        registerAllocationCommand(MoveAllocationCommand::new, MoveAllocationCommand::fromXContent,
            MoveAllocationCommand.COMMAND_NAME_FIELD);
        registerAllocationCommand(AllocateReplicaAllocationCommand::new, AllocateReplicaAllocationCommand::fromXContent,
            AllocateReplicaAllocationCommand.COMMAND_NAME_FIELD);
        registerAllocationCommand(AllocateEmptyPrimaryAllocationCommand::new, AllocateEmptyPrimaryAllocationCommand::fromXContent,
            AllocateEmptyPrimaryAllocationCommand.COMMAND_NAME_FIELD);
        registerAllocationCommand(AllocateStalePrimaryAllocationCommand::new, AllocateStalePrimaryAllocationCommand::fromXContent,
            AllocateStalePrimaryAllocationCommand.COMMAND_NAME_FIELD);
    }

    private final Map<String, Supplier<HttpServerTransport>> transportHttpFactories = new HashMap<>();

    /**
     * Creates a network module that custom networking classes can be plugged into.
     * @param settings The settings for the node
     */
    public NetworkModule(Settings settings,
                         List<NetworkPlugin> plugins,
                         ThreadPool threadPool,
                         BigArrays bigArrays,
                         PageCacheRecycler pageCacheRecycler,
                         CircuitBreakerService circuitBreakerService,
                         NamedWriteableRegistry namedWriteableRegistry,
                         NamedXContentRegistry xContentRegistry,
                         NetworkService networkService,
                         NettyBootstrap nettyBootstrap,
                         Authentication authentication,
                         SslContextProvider sslContextProvider,
                         NodeClient nodeClient) {
        this.settings = settings;
        for (NetworkPlugin plugin : plugins) {
            Map<String, Supplier<HttpServerTransport>> httpTransportFactory = plugin.getHttpTransports(
                settings,
                threadPool,
                bigArrays,
                circuitBreakerService,
                namedWriteableRegistry,
                xContentRegistry,
                networkService,
                nettyBootstrap,
                nodeClient
            );
            for (Map.Entry<String, Supplier<HttpServerTransport>> entry : httpTransportFactory.entrySet()) {
                registerHttpTransport(entry.getKey(), entry.getValue());
            }
        }
    }

    /** Adds an http transport implementation that can be selected by setting {@link #HTTP_TYPE_KEY}. */
    // TODO: we need another name than "http transport"....so confusing with transportClient...
    private void registerHttpTransport(String key, Supplier<HttpServerTransport> factory) {
        if (transportHttpFactories.putIfAbsent(key, factory) != null) {
            throw new IllegalArgumentException("transport for name: " + key + " is already registered");
        }
    }

    /**
     * Register an allocation command.
     * <p>
     * This lives here instead of the more aptly named ClusterModule because the Transport client needs these to be registered.
     * </p>
     * @param reader the reader to read it from a stream
     * @param parser the parser to read it from XContent
     * @param commandName the names under which the command should be parsed. The {@link ParseField#getPreferredName()} is special because
     *        it is the name under which the command's reader is registered.
     */
    private static <T extends AllocationCommand> void registerAllocationCommand(Writeable.Reader<T> reader,
            CheckedFunction<XContentParser, T, IOException> parser, ParseField commandName) {
        NAMED_XCONTENTS.add(new NamedXContentRegistry.Entry(AllocationCommand.class, commandName, parser));
        NAMED_WRITEABLES.add(new NamedWriteableRegistry.Entry(AllocationCommand.class, commandName.getPreferredName(), reader));
    }

    public static List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return Collections.unmodifiableList(NAMED_WRITEABLES);
    }

    public static List<NamedXContentRegistry.Entry> getNamedXContents() {
        return Collections.unmodifiableList(NAMED_XCONTENTS);
    }

    public Supplier<HttpServerTransport> getHttpServerTransportSupplier() {
        final String name;
        if (HTTP_TYPE_SETTING.exists(settings)) {
            name = HTTP_TYPE_SETTING.get(settings);
        } else {
            name = HTTP_DEFAULT_TYPE_SETTING.get(settings);
        }
        final Supplier<HttpServerTransport> factory = transportHttpFactories.get(name);
        if (factory == null) {
            throw new IllegalStateException("Unsupported http.type [" + name + "]");
        }
        return factory;
    }
}
