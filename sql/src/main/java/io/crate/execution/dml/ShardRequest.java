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

package io.crate.execution.dml;

import com.google.common.base.Objects;
import com.google.common.collect.Iterators;
import io.crate.Constants;
import org.elasticsearch.action.support.replication.ReplicatedWriteRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

public abstract class ShardRequest<T extends ShardRequest<T, I>, I extends ShardRequest.Item>
    extends ReplicatedWriteRequest<T> implements Iterable<I> {

    @Nullable
    private String routing;
    private UUID jobId;
    protected List<I> items;

    public ShardRequest() {
    }

    public ShardRequest(ShardId shardId,
                        @Nullable String routing,
                        UUID jobId) {
        setShardId(shardId);
        this.routing = routing;
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
        return Iterators.unmodifiableIterator(items.iterator());
    }

    public String type() {
        return Constants.DEFAULT_MAPPING_TYPE;
    }

    @Nullable
    public String routing() {
        return routing;
    }

    public UUID jobId() {
        return jobId;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        routing = in.readOptionalString();
        jobId = new UUID(in.readLong(), in.readLong());
    }

    protected void readItems(StreamInput in, int size) throws IOException {
        items = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            items.add(readItem(in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(routing);
        out.writeLong(jobId.getMostSignificantBits());
        out.writeLong(jobId.getLeastSignificantBits());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ShardRequest<?, ?> that = (ShardRequest<?, ?>) o;
        return Objects.equal(routing, that.routing) &&
               Objects.equal(jobId, that.jobId) &&
               Objects.equal(items, that.items);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(routing, jobId, shardId(), items);
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

    protected abstract I readItem(StreamInput input) throws IOException;

    /**
     * A single item with just an id and an optional location.
     */
    public abstract static class Item implements Writeable {

        protected final String id;
        private int location = -1;
        private long seqNo = SequenceNumbers.UNASSIGNED_SEQ_NO;

        public Item(String id) {
            this.id = id;
        }

        protected Item(StreamInput in) throws IOException {
            id = in.readString();
            location = in.readInt();
        }

        public String id() {
            return id;
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

        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(id);
            out.writeInt(location);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Item item = (Item) o;
            return location == item.location && id.equals(item.id);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(id, location);
        }

        @Override
        public String toString() {
            return "Item{" +
                   "id='" + id + '\'' +
                   ", location=" + location +
                   '}';
        }
    }

}
