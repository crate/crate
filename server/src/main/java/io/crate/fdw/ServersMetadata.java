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

import static java.util.Objects.requireNonNull;
import static org.elasticsearch.common.xcontent.XContentParser.Token.END_OBJECT;
import static org.elasticsearch.common.xcontent.XContentParser.Token.FIELD_NAME;
import static org.elasticsearch.common.xcontent.XContentParser.Token.START_OBJECT;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Stream;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.Metadata.XContentContext;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;

import io.crate.fdw.ServersMetadata.Server;
import io.crate.fdw.ServersMetadata.Server.Option;
import io.crate.metadata.information.UserMappingOptionsTableInfo;
import io.crate.metadata.information.UserMappingsTableInfo.UserMapping;
import io.crate.server.xcontent.XContentParserUtils;
import io.crate.sql.tree.CascadeMode;
import io.crate.types.DataTypes;

public final class ServersMetadata extends AbstractNamedDiffable<Metadata.Custom>
    implements Metadata.Custom, Iterable<Server> {

    public static final String TYPE = "servers";
    public static final ServersMetadata EMPTY = new ServersMetadata(Map.of());

    public record Server(String name,
                         String fdw,
                         String owner,
                         Map<String, Settings> users,
                         Settings options) implements Writeable {


        public Server(StreamInput in) throws IOException {
            this(
                in.readString(),
                in.readString(),
                in.readString(),
                in.readMap(StreamInput::readString, Settings::readSettingsFromStream),
                Settings.readSettingsFromStream(in)
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeString(fdw);
            out.writeString(owner);
            out.writeMap(users, StreamOutput::writeString, Settings::writeSettingsToStream);
            Settings.writeSettingsToStream(out, options);
        }

        public static Server fromXContent(XContentParser parser) throws IOException {
            String name = null;
            String fdw = null;
            String owner = null;
            Map<String, Settings> users = new HashMap<>();
            Settings options = null;
            while (parser.nextToken() != END_OBJECT) {
                if (parser.currentToken() == FIELD_NAME) {
                    String fieldName = parser.currentName();
                    parser.nextToken();
                    switch (fieldName) {
                        case "name":
                            name = parser.text();
                            break;

                        case "fdw":
                            fdw = parser.text();
                            break;

                        case "owner":
                            owner = parser.text();
                            break;

                        case "users":
                            XContentParserUtils.ensureExpectedToken(START_OBJECT, parser.currentToken(), parser);
                            parser.nextToken();
                            while (parser.currentToken() == FIELD_NAME) {
                                parser.nextToken();
                                String key = parser.currentName();
                                Settings settings = Settings.fromXContent(parser);
                                users.put(key, settings);
                                XContentParserUtils.ensureExpectedToken(END_OBJECT, parser.currentToken(), parser);
                                parser.nextToken();
                            }
                            XContentParserUtils.ensureExpectedToken(END_OBJECT, parser.currentToken(), parser);
                            break;

                        case "options":
                            options = Settings.fromXContent(parser);
                            break;

                        default:
                            // skip over unknown fields for forward compatibility
                            parser.skipChildren();
                    }
                }
            }
            return new Server(
                requireNonNull(name),
                requireNonNull(fdw),
                requireNonNull(owner),
                requireNonNull(users),
                requireNonNull(options));
        }

        public record Option(String serverName,
                             String serverOwner,
                             String name,
                             String value) {
        }

        public Stream<Option> getOptions() {
            return options.getAsStructuredMap().entrySet().stream()
                .map(x -> new Option(name, owner, x.getKey(), DataTypes.STRING.implicitCast(x.getValue())));
        }

        public Server withOptions(Settings options) {
            return new Server(name, fdw, owner, users, options);
        }
    }

    private final Map<String, Server> servers;

    ServersMetadata(Map<String, Server> servers) {
        this.servers = servers;
    }

    public ServersMetadata(StreamInput in) throws IOException {
        this.servers = in.readMap(StreamInput::readString, Server::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(servers, StreamOutput::writeString, (o, value) -> value.writeTo(o));
    }

    public static ServersMetadata fromXContent(XContentParser parser) throws IOException {
        Map<String, Server> servers = new HashMap<>();
        if (parser.currentToken() == START_OBJECT) {
            parser.nextToken();
        }
        if (parser.currentToken() == FIELD_NAME) {
            assert parser.currentName().endsWith(TYPE) : "toXContent starts with startObject(TYPE)";
            parser.nextToken();
        }
        while (parser.nextToken() != END_OBJECT) {
            if (parser.currentToken() == FIELD_NAME) {
                String serverName = parser.currentName();
                parser.nextToken();
                Server server = Server.fromXContent(parser);
                servers.put(serverName, server);
            }
        }
        parser.nextToken();
        return new ServersMetadata(servers);
    }


    public boolean contains(String name) {
        return servers.containsKey(name);
    }

    public ServersMetadata add(String name,
                               String fdw,
                               String owner,
                               Settings options) {
        Server server = new Server(name, fdw, owner, Map.of(), options);
        return put(name, server);
    }

    public ServersMetadata put(String name,
                               Server server) {
        HashMap<String, Server> servers = new HashMap<>(this.servers);
        servers.put(name, server);
        return new ServersMetadata(servers);
    }

    public ServersMetadata addUser(String serverName,
                                   boolean ifNotExists,
                                   String userName,
                                   Settings options) {
        Server server = get(serverName);
        if (server.users.containsKey(userName)) {
            if (ifNotExists) {
                return this;
            }
            throw new UserMappingAlreadyExists(userName, serverName);
        }
        HashMap<String, Server> newServers = new HashMap<>(this.servers);
        HashMap<String, Settings> newUsers = new HashMap<>(server.users);
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
        HashMap<String, Settings> newUsers = new HashMap<>(server.users);
        Settings removed = newUsers.remove(userName);
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

    @Override
    public int hashCode() {
        return servers.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof ServersMetadata other
            && servers.equals(other.servers);
    }

    public Iterable<UserMapping> getUserMappings() {
        return () ->
            servers.values().stream()
                .map(server -> server)
                .flatMap(server -> server.users.keySet().stream()
                    .map(userName -> new UserMapping(userName, server.name()))
                ).iterator();
    }

    public Iterable<UserMappingOptionsTableInfo.UserMappingOptions> getUserMappingOptions() {
        return () ->
            servers.values().stream()
                .map(server -> server)
                .flatMap(server -> server.users.entrySet().stream()
                    .flatMap(e -> e.getValue().getAsStructuredMap().entrySet().stream()
                        .map(setting -> new UserMappingOptionsTableInfo.UserMappingOptions(
                            e.getKey(), server.name(), setting.getKey(), setting.getValue().toString())
                        ))).iterator();
    }

    public Iterable<Option> getOptions() {
        return () -> servers.values().stream()
            .flatMap(server -> server.getOptions())
            .iterator();
    }
}
