/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.executor.transport;

import com.google.common.base.Objects;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.UUID;

public class ShardDeleteRequest extends ShardRequest<ShardDeleteRequest, ShardDeleteRequest.Item> {

    private int skipFromLocation = -1;

    public ShardDeleteRequest() {
    }

    public ShardDeleteRequest(ShardId shardId, @Nullable String routing, UUID jobId) {
        super(shardId, routing, jobId);
    }

    void skipFromLocation(int location) {
        skipFromLocation = location;
    }

    int skipFromLocation() {
        return skipFromLocation;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        writeItems(out);
        if (skipFromLocation > -1) {
            out.writeBoolean(true);
            out.writeVInt(skipFromLocation);
        } else {
            out.writeBoolean(false);
        }
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        readItems(in, locations.size());
        if (in.readBoolean()) {
            skipFromLocation = in.readVInt();
        }
    }

    @Override
    protected Item readItem(StreamInput input) throws IOException {
        Item item = new Item();
        item.readFrom(input);
        return item;
    }

    public static class Item extends ShardRequest.Item {

        private long version = Versions.MATCH_ANY;
        private VersionType versionType = VersionType.INTERNAL;

        protected Item() {
            super();
        }

        public Item(String id) {
            super(id);
        }

        public long version() {
            return version;
        }

        public void version(long version) {
            this.version = version;
        }

        public VersionType versionType() {
            return versionType;
        }

        public void versionType(VersionType versionType) {
            this.versionType = versionType;
        }

        @Override
        public boolean equals(Object o) {
            if (!super.equals(o)) return false;
            if (this == o) return true;
            if (getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            Item item = (Item) o;
            return version == item.version &&
                   versionType == item.versionType;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(super.hashCode(), version, versionType);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            version = in.readLong();
            versionType = VersionType.fromValue(in.readByte());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeLong(version);
            out.writeByte(versionType.getValue());
        }
    }

    public static class Builder implements BulkRequestBuilder<ShardDeleteRequest> {

        private final TimeValue timeout;
        private final UUID jobId;

        public Builder(TimeValue timeout, UUID jobId) {
            this.timeout = timeout;
            this.jobId = jobId;
        }

        @Override
        public ShardDeleteRequest newRequest(ShardId shardId, String routing) {
            return new ShardDeleteRequest(
                shardId,
                routing,
                jobId)
                .timeout(timeout);
        }
    }

}
