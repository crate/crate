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

public class ServersMetadata extends AbstractNamedDiffable<Metadata.Custom> implements Metadata.Custom {

    public static final String TYPE = "servers";
    public static final ServersMetadata EMPTY = new ServersMetadata(Map.of());


    public record Server(String fdw, Map<String, Object> options) implements Writeable, ToXContent {

        public Server(StreamInput in) throws IOException {
            this(
                in.readString(),
                in.readMap(StreamInput::readString, StreamInput::readGenericValue)
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(fdw);
            out.writeMap(options, StreamOutput::writeString, StreamOutput::writeGenericValue);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field("fdw", fdw);
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

    public ServersMetadata add(String name, String fdw, Map<String, Object> options) {
        HashMap<String, Server> servers = new HashMap<>(this.servers);
        servers.put(name, new Server(fdw, options));
        return new ServersMetadata(servers);
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
}
