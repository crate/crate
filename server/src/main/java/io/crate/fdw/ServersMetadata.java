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

package io.crate.fdw;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.Metadata.XContentContext;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import io.crate.fdw.ServersMetadata.Server;
import io.crate.sql.tree.CascadeMode;

public class ServersMetadata extends AbstractNamedDiffable<Metadata.Custom>
    implements Metadata.Custom, Iterable<Server> {

    public static final String TYPE = "servers";
    public static final ServersMetadata EMPTY = new ServersMetadata(Map.of());


    public record Server(String name,
                         String fdw,
                         String owner,
                         Map<String, Map<String, Object>> users,
                         Map<String, Object> options) implements Writeable, ToXContent {

        public Server(StreamInput in) throws IOException {
            this(
                in.readString(),
                in.readString(),
                in.readString(),
                in.readMap(StreamInput::readString, StreamInput::readMap),
                in.readMap(StreamInput::readString, StreamInput::readGenericValue)
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeString(fdw);
            out.writeString(owner);
            out.writeMap(users, StreamOutput::writeString, StreamOutput::writeMap);
            out.writeMap(options, StreamOutput::writeString, StreamOutput::writeGenericValue);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field("name", name);
            builder.field("fdw", fdw);
            builder.field("owner", owner);
            builder.field("users", users);
            builder.field("options", options);
            return builder;
        }
    }

    private final Map<String, Server> servers;

    private ServersMetadata(Map<String, Server> servers) {
        this.servers = servers;
    }

    public ServersMetadata(StreamInput in) throws IOException {
        this.servers = in.readMap(StreamInput::readString, Server::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(servers, StreamOutput::writeString, (o, value) -> value.writeTo(o));
    }

    public boolean contains(String name) {
        return servers.containsKey(name);
    }

    public ServersMetadata add(String name,
                               String fdw,
                               String owner,
                               Map<String, Object> options) {
        HashMap<String, Server> servers = new HashMap<>(this.servers);
        Server server = new Server(name, fdw, owner, Map.of(), options);
        servers.put(name, server);
        return new ServersMetadata(servers);
    }

    public ServersMetadata addUser(String serverName,
                                   boolean ifNotExists,
                                   String userName,
                                   Map<String, Object> options) {
        Server server = get(serverName);
        if (server.users.containsKey(userName)) {
            if (ifNotExists) {
                return this;
            }
            throw new UserMappingAlreadyExists(userName, serverName);
        }
        HashMap<String, Server> newServers = new HashMap<>(this.servers);
        HashMap<String, Map<String, Object>> newUsers = new HashMap<>(server.users);
        newUsers.put(userName, options);
        Server newServer = new Server(serverName, server.fdw, server.owner, newUsers, server.options);
        newServers.put(serverName, newServer);
        return new ServersMetadata(newServers);
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_5_7_0;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(TYPE);
        for (var entry : servers.entrySet()) {
            String serverName = entry.getKey();
            Server server = entry.getValue();
            builder.startObject(serverName);
            server.toXContent(builder, params);
            builder.endObject();
        }
        return builder.endObject();
    }

    @Override
    public EnumSet<XContentContext> context() {
        return EnumSet.of(Metadata.XContentContext.GATEWAY, Metadata.XContentContext.SNAPSHOT);
    }

    /**
     * @throws ResourceNotFoundException if server is not found
     */
    public Server get(String serverName) {
        Server server = servers.get(serverName);
        if (server == null) {
            throw new ResourceNotFoundException(String.format(
                Locale.ENGLISH,
                "Server `%s` not found",
                serverName
            ));
        }
        return server;
    }

    @Override
    public Iterator<Server> iterator() {
        return servers.values().iterator();
    }

    public ServersMetadata remove(List<String> names, boolean ifExists, CascadeMode mode) {
        Map<String, Server> newServers = new HashMap<>(this.servers);
        for (String serverName : names) {
            Server removed = newServers.remove(serverName);
            if (removed == null) {
                if (!ifExists) {
                    throw new ResourceNotFoundException(String.format(
                        Locale.ENGLISH,
                        "Server `%s` not found",
                        serverName
                    ));
                }
            } else if (mode == CascadeMode.RESTRICT && !removed.users().isEmpty()) {
                throw new IllegalArgumentException(String.format(
                    Locale.ENGLISH,
                    "Cannot drop server `%s` because mapped users (%s) depend on it",
                    serverName,
                    String.join(", ", removed.users().keySet())
                ));
            }
        }
        return newServers.size() == servers.size() ? this : new ServersMetadata(newServers);
    }

    public ServersMetadata dropUser(String serverName, String userName, boolean ifExists) {
        Server server = get(serverName);
        HashMap<String, Map<String, Object>> newUsers = new HashMap<>(server.users);
        Map<String, Object> removed = newUsers.remove(userName);
        if (removed == null && !ifExists) {
            throw new ResourceNotFoundException(String.format(
                Locale.ENGLISH,
                "No user mapping found for user `%s` and server `%s`",
                userName,
                serverName
            ));
        }
        if (newUsers.size() == server.users.size()) {
            return this;
        }
        HashMap<String, Server> newServers = new HashMap<>(servers);
        Server newServer = new Server(serverName, server.fdw, server.owner, newUsers, server.options);
        newServers.replace(serverName, newServer);
        return new ServersMetadata(newServers);
    }
}
