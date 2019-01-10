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

package org.elasticsearch.snapshots;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent.Params;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/**
 * SnapshotId - snapshot name + snapshot UUID
 */
public final class SnapshotId implements Comparable<SnapshotId>, Writeable, ToXContentObject {

    private static final String NAME = "name";
    private static final String UUID = "uuid";

    private final String name;
    private final String uuid;

    // Caching hash code
    private final int hashCode;

    /**
     * Constructs a new snapshot
     *
     * @param name   snapshot name
     * @param uuid   snapshot uuid
     */
    public SnapshotId(final String name, final String uuid) {
        this.name = Objects.requireNonNull(name);
        this.uuid = Objects.requireNonNull(uuid);
        this.hashCode = computeHashCode();
    }

    /**
     * Constructs a new snapshot from a input stream
     *
     * @param in  input stream
     */
    public SnapshotId(final StreamInput in) throws IOException {
        name = in.readString();
        uuid = in.readString();
        hashCode = computeHashCode();
    }

    /**
     * Returns snapshot name
     *
     * @return snapshot name
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the snapshot UUID
     *
     * @return snapshot uuid
     */
    public String getUUID() {
        return uuid;
    }

    @Override
    public String toString() {
        return name + "/" + uuid;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        @SuppressWarnings("unchecked") final SnapshotId that = (SnapshotId) o;
        return name.equals(that.name) && uuid.equals(that.uuid);
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public int compareTo(final SnapshotId other) {
        return this.name.compareTo(other.name);
    }

    private int computeHashCode() {
        return Objects.hash(name, uuid);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeString(uuid);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(NAME, name);
        builder.field(UUID, uuid);
        builder.endObject();
        return builder;
    }

    public static SnapshotId fromXContent(XContentParser parser) throws IOException {
        // the new format from 5.0 which contains the snapshot name and uuid
        if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
            String name = null;
            String uuid = null;
            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                String currentFieldName = parser.currentName();
                parser.nextToken();
                if (NAME.equals(currentFieldName)) {
                    name = parser.text();
                } else if (UUID.equals(currentFieldName)) {
                    uuid = parser.text();
                }
            }
            return new SnapshotId(name, uuid);
        } else {
            // the old format pre 5.0 that only contains the snapshot name, use the name as the uuid too
            final String name = parser.text();
            return new SnapshotId(name, name);
        }
    }

}
