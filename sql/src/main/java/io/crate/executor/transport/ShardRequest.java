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

import com.carrotsearch.hppc.IntArrayList;
import com.google.common.base.Objects;
import com.google.common.collect.Iterators;
import io.crate.Constants;
import org.elasticsearch.action.support.replication.ReplicationRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

public abstract class ShardRequest<T extends ReplicationRequest<T>, I extends ShardRequest.Item>
        extends ReplicationRequest<T> implements Iterable<I> {

    @Nullable
    private String routing;
    private UUID jobId;
    private List<I> items;
    protected IntArrayList locations;

    public ShardRequest() {
    }

    public ShardRequest(ShardId shardId,
                        @Nullable String routing,
                        UUID jobId) {
        setShardId(shardId);
        this.routing = routing;
        this.jobId = jobId;
        this.index = shardId.getIndex();
        locations = new IntArrayList();
        items = new ArrayList<>();
    }

    public void add(int location, I item) {
        locations.add(location);
        items.add(item);
    }

    public List<I> items() {
        return items;
    }

    @Override
    public Iterator<I> iterator() {
        return Iterators.unmodifiableIterator(items.iterator());
    }

    public IntArrayList itemIndices() {
        return locations;
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

        int size = in.readVInt();
        locations = new IntArrayList(size);
        for (int i = 0; i < size; i++) {
            locations.add(in.readVInt());
        }
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
        out.writeVInt(locations.size());
        for (int i = 0; i < locations.size(); i++) {
            out.writeVInt(locations.get(i));
        }
    }

    protected void writeItems(StreamOutput out) throws IOException {
        for (Item item : items) {
            item.writeTo(out);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ShardRequest<?, ?> that = (ShardRequest<?, ?>) o;
        return Objects.equal(routing, that.routing) &&
               Objects.equal(jobId, that.jobId) &&
               Objects.equal(items, that.items) &&
               Objects.equal(locations, that.locations);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(routing, jobId, shardId(), items, locations);
    }

    protected abstract I readItem(StreamInput input) throws IOException;

    /**
     * A single item with just an id.
     */
    public static class Item implements Streamable {

        protected String id;

        protected Item() {
        }

        public Item(String id) {
            this.id = id;
        }

        public String id() {
            return id;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            id = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(id);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Item item = (Item) o;
            return Objects.equal(id, item.id);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(id);
        }
    }

}
