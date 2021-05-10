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

package io.crate.execution.dml;

import org.elasticsearch.action.support.replication.ReplicationRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

public abstract class ShardRequest<T extends ShardRequest<T, I>, I extends ShardRequest.Item>
    extends ReplicationRequest<T> implements Iterable<I> {

    private UUID jobId;
    protected List<I> items;

    public ShardRequest() {
    }

    public ShardRequest(ShardId shardId, UUID jobId) {
        setShardId(shardId);
        this.jobId = jobId;
        this.index = shardId.getIndexName();
        items = new ArrayList<>();
    }

    public void add(int location, I item) {
        item.location(location);
        items.add(item);
    }

    public List<I> items() {
        return items;
    }

    @Override
    public Iterator<I> iterator() {
        return Collections.unmodifiableCollection(items).iterator();
    }

    public UUID jobId() {
        return jobId;
    }

    public ShardRequest(StreamInput in) throws IOException {
        super(in);
        jobId = new UUID(in.readLong(), in.readLong());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeLong(jobId.getMostSignificantBits());
        out.writeLong(jobId.getLeastSignificantBits());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ShardRequest<?, ?> that = (ShardRequest<?, ?>) o;
        return Objects.equals(jobId, that.jobId) &&
               Objects.equals(items, that.items);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, items);
    }

    /**
     * The description is used when creating transport, replication and search tasks and it defaults to `toString`.
     * Only return the shard id to avoid the overhead of including all the items.
     */

    @Override
    public String toString() {
        return "ShardRequest{" +
               ", shardId=" + shardId +
               ", timeout=" + timeout +
               '}';
    }

    public abstract static class Item implements Writeable {

        protected final String id;
        protected long version = Versions.MATCH_ANY;

        private int location = -1;
        protected long seqNo = SequenceNumbers.UNASSIGNED_SEQ_NO;
        protected long primaryTerm = SequenceNumbers.UNASSIGNED_PRIMARY_TERM;

        public Item(String id) {
            this.id = id;
        }

        protected Item(StreamInput in) throws IOException {
            id = in.readString();
            version = in.readLong();
            location = in.readInt();
            seqNo = in.readLong();
            primaryTerm = in.readLong();
        }

        public String id() {
            return id;
        }

        public long version() {
            return version;
        }

        public void version(long version) {
            this.version = version;
        }

        public void location(int location) {
            this.location = location;
        }

        public int location() {
            return location;
        }

        public long seqNo() {
            return seqNo;
        }

        public void seqNo(long seqNo) {
            this.seqNo = seqNo;
        }

        public long primaryTerm() {
            return primaryTerm;
        }

        public void primaryTerm(long primaryTerm) {
            this.primaryTerm = primaryTerm;
        }

        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(id);
            out.writeLong(version);
            out.writeInt(location);
            out.writeLong(seqNo);
            out.writeLong(primaryTerm);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Item item = (Item) o;
            return version == item.version &&
                   location == item.location &&
                   seqNo == item.seqNo &&
                   primaryTerm == item.primaryTerm &&
                   java.util.Objects.equals(id, item.id);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(id, version, location, seqNo, primaryTerm);
        }

        @Override
        public String toString() {
            return "Item{" +
                   "id='" + id + '\'' +
                   ", version=" + version +
                   ", location=" + location +
                   ", seqNo=" + seqNo +
                   ", primaryTerm=" + primaryTerm +
                   '}';
        }
    }

}
