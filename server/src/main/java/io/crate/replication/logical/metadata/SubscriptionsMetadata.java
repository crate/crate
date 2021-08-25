/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.replication.logical.metadata;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class SubscriptionsMetadata extends AbstractNamedDiffable<Metadata.Custom> implements Metadata.Custom {

    public static final String TYPE = "subscriptions";

    public static SubscriptionsMetadata newInstance(SubscriptionsMetadata instance) {
        if (instance == null) {
            return new SubscriptionsMetadata();
        }
        return new SubscriptionsMetadata(new HashMap<>(instance.subscriptionByName));
    }

    private final Map<String, Subscription> subscriptionByName;

    public SubscriptionsMetadata(Map<String, Subscription> subscriptionByName) {
        this.subscriptionByName = subscriptionByName;
    }

    public SubscriptionsMetadata(StreamInput in) throws IOException {
        int numPubs = in.readVInt();
        subscriptionByName = new HashMap<>(numPubs);
        for (int i = 0; i < numPubs; i++) {
            subscriptionByName.put(in.readString(), new Subscription(in));
        }
    }

    private SubscriptionsMetadata() {
        this(new HashMap<>());
    }

    public Map<String, Subscription> subscription() {
        return subscriptionByName;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(subscriptionByName.size());
        for (var entry : subscriptionByName.entrySet()) {
            out.writeString(entry.getKey());
            entry.getValue().writeTo(out);
        }
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        return EnumSet.of(Metadata.XContentContext.GATEWAY, Metadata.XContentContext.SNAPSHOT);
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_4_7_0;
    }

    /*
     * PublicationsMetadata XContent has the following structure:
     *
     * <pre>
     *     {
     *       "publications": {
     *         "my_pub1": {
     *           "owner": "user1",
     *           "tables": [ "table1", "table2" ]
     *         }
     *       }
     *     }
     * </pre>
     *
     * <ul>
     *     <li>"my_pub1" is the full qualified name of the publication</li>
     *     <li>value of "owner" is the name of the user who created the publication</li>
     *     <li>value of "tables" contains a list of all tables which are part of this publication</li>
     * </ul>
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(TYPE);
        for (var entry : subscriptionByName.entrySet()) {
            var subscription = entry.getValue();
            builder.startObject(entry.getKey());
            {
                builder.field("owner", subscription.owner());

                builder.startObject("connection_info");
                {
                    builder.field("hosts", subscription.connectionInfo().hosts().toArray(new String[0]));
                    builder.startObject("settings");
                    subscription.connectionInfo().settings().toXContent(builder, params);
                    builder.endObject();
                }
                builder.endObject();

                builder.array("publications", subscription.publications().toArray(new String[0]));
                builder.startObject("settings");
                subscription.settings().toXContent(builder, params);
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    public static SubscriptionsMetadata fromXContent(XContentParser parser) throws IOException {
        Map<String, Subscription> subscriptions = new HashMap<>();

        if (parser.nextToken() == XContentParser.Token.FIELD_NAME && parser.currentName().equals(TYPE)) {
            if (parser.nextToken() == XContentParser.Token.START_OBJECT) {
                while (parser.nextToken() == XContentParser.Token.FIELD_NAME) {
                    String name = parser.currentName();
                    if (parser.nextToken() == XContentParser.Token.START_OBJECT) {
                        String owner = null;
                        ConnectionInfo connectionInfo = null;
                        Settings settings = null;
                        var publications = new ArrayList<String>();
                        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                            if ("owner".equals(parser.currentName())) {
                                parser.nextToken();
                                owner = parser.textOrNull();
                            }
                            if ("connection_info".equals(parser.currentName())
                                && parser.nextToken() == XContentParser.Token.START_OBJECT) {
                                ArrayList<String> hosts = new ArrayList<>();
                                Settings connSettings = null;
                                while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                                    if ("hosts".equals(parser.currentName())) {
                                        parser.nextToken();
                                        while ((parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                                            hosts.add(parser.text());
                                        }
                                    }
                                    if ("settings".equals(parser.currentName())) {
                                        parser.nextToken();
                                        connSettings = Settings.fromXContent(parser);
                                    }
                                }
                                connectionInfo = new ConnectionInfo(hosts, connSettings);
                            }
                            if ("publications".equals(parser.currentName())) {
                                parser.nextToken();
                                while ((parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                                    publications.add(parser.text());
                                }
                            }
                            if ("settings".equals(parser.currentName())) {
                                parser.nextToken();
                                settings = Settings.fromXContent(parser);
                            }
                        }
                        if (owner == null) {
                            throw new ElasticsearchParseException("failed to parse subscription, expected field 'owner' in object");
                        }
                        if (connectionInfo == null) {
                            throw new ElasticsearchParseException("failed to parse subscription, expected field 'connection_info' in object");
                        }
                        if (publications.isEmpty()) {
                            throw new ElasticsearchParseException("failed to parse subscription, expected field 'publications' in object");
                        }
                        if (settings == null) {
                            throw new ElasticsearchParseException("failed to parse subscription, expected field 'settings' in object");
                        }
                        subscriptions.put(name, new Subscription(owner, connectionInfo, publications, settings));
                    }
                }
            }
            if (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                // each custom metadata is packed inside an object.
                // each custom must move the parser to the end otherwise possible following customs won't be read
                throw new ElasticsearchParseException("failed to parse subscriptions, expected an object token at the end");
            }
        }
        return new SubscriptionsMetadata(subscriptions);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SubscriptionsMetadata that = (SubscriptionsMetadata) o;
        return subscriptionByName.equals(that.subscriptionByName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(subscriptionByName);
    }
}
