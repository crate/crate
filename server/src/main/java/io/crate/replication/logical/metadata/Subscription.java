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

import io.crate.metadata.RelationName;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;

import org.jetbrains.annotations.Nullable;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class Subscription implements Writeable {

    public enum State {
        INITIALIZING("i"),
        RESTORING("d"),
        MONITORING("r"),
        FAILED("e");

        private static final State[] VALUES = State.values();

        private final String pg_state;

        State(String pg_state) {
            this.pg_state = pg_state;
        }

        public String pg_state() {
            return pg_state;
        }
    }

    public static RelationState updateRelationState(@Nullable RelationState oldState, RelationState newState) {
        if (oldState == null) {
            return newState;
        }
        // Do not override existing same states, e.g. do not override a failed state with a new reason to preserve
        // existing causes
        if (oldState.state.equals(newState.state)) {
            return oldState;
        }
        return newState;
    }

    public record RelationState(State state, @Nullable String reason) {

        public static void writeTo(StreamOutput out, RelationState relationState) throws IOException {
            out.writeVInt(relationState.state.ordinal());
            out.writeOptionalString(relationState.reason);
        }

        public static RelationState readFrom(StreamInput in) throws IOException {
            return new RelationState(
                State.VALUES[in.readVInt()],
                in.readOptionalString()
            );
        }
    }

    private final String owner;
    private final ConnectionInfo connectionInfo;
    private final List<String> publications;
    private final Settings settings;
    private final Map<RelationName, RelationState> relations;

    public Subscription(String owner,
                        ConnectionInfo connectionInfo,
                        List<String> publications,
                        Settings settings,
                        Map<RelationName, RelationState> relations) {
        this.owner = owner;
        this.connectionInfo = connectionInfo;
        this.publications = publications;
        this.settings = settings;
        this.relations = relations;
    }

    public Subscription(StreamInput in) throws IOException {
        owner = in.readString();
        connectionInfo = new ConnectionInfo(in);
        publications = Arrays.stream(in.readStringArray()).toList();
        settings = Settings.readSettingsFromStream(in);
        int relSize = in.readVInt();
        HashMap<RelationName, RelationState> relations = new HashMap<>();
        for (int i = 0; i < relSize; i++) {
            RelationName relationName = new RelationName(in);
            RelationState relationState = RelationState.readFrom(in);
            relations.put(relationName, relationState);
        }
        this.relations = relations;
    }

    public String owner() {
        return owner;
    }

    public ConnectionInfo connectionInfo() {
        return connectionInfo;
    }

    public List<String> publications() {
        return publications;
    }

    public Settings settings() {
        return settings;
    }

    public Map<RelationName, RelationState> relations() {
        return relations;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(owner);
        connectionInfo.writeTo(out);
        out.writeStringArray(publications.toArray(new String[0]));
        Settings.writeSettingsToStream(out, settings);
        out.writeVInt(relations.size());
        for (var entry : relations.entrySet()) {
            entry.getKey().writeTo(out);
            RelationState.writeTo(out, entry.getValue());
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Subscription that = (Subscription) o;
        return owner.equals(that.owner) && connectionInfo.equals(that.connectionInfo) &&
               publications.equals(that.publications) && settings.equals(that.settings) &&
               relations.equals(that.relations);
    }

    @Override
    public int hashCode() {
        return Objects.hash(owner, connectionInfo, publications, settings, relations);
    }

    @Override
    public String toString() {
        return "Subscription{" +
               "owner='" + owner + '\'' +
               ", connectionInfo=" + connectionInfo +
               ", publications=" + publications +
               ", settings=" + settings +
               ", relations=" + relations +
               '}';
    }
}
