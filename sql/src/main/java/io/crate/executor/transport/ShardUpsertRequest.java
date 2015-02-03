/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.executor.transport;

import com.carrotsearch.hppc.IntArrayList;
import com.google.common.collect.Iterators;
import io.crate.Constants;
import io.crate.Streamer;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import org.elasticsearch.action.support.replication.ShardReplicationOperationRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ShardUpsertRequest extends ShardReplicationOperationRequest<ShardUpsertRequest> implements Iterable<ShardUpsertRequest.Item> {

    /**
     * A single update item.
     */
    public static class Item implements Streamable {

        private String id;
        private String routing;
        private Symbol[] assignments;
        private long version = Versions.MATCH_ANY;
        @Nullable
        private Object[] missingAssignments;
        @Nullable
        private Streamer[] streamers;


        Item(@Nullable Streamer[] streamers) {
            this.streamers = streamers;
        }

        Item(String id,
             @Nullable Symbol[] assignments,
             @Nullable Object[] missingAssignments,
             @Nullable Long version,
             @Nullable String routing,
             @Nullable Streamer[] streamers) {
            this(streamers);
            this.id = id;
            this.routing = routing;
            this.assignments = assignments;
            if (version != null) {
                this.version = version;
            }
            this.missingAssignments = missingAssignments;
        }

        public String id() {
            return id;
        }

        @Nullable
        public String routing() {
            return routing;
        }

        public long version() {
            return version;
        }

        public int retryOnConflict() {
            return version == Versions.MATCH_ANY ? Constants.UPDATE_RETRY_ON_CONFLICT : 0;
        }

        @Nullable
        public Symbol[] assignments() {
            return assignments;
        }

        @Nullable
        public Object[] missingAssignments() {
            return missingAssignments;
        }

        static Item readItem(StreamInput in, @Nullable Streamer[] streamers) throws IOException {
            Item item = new Item(streamers);
            item.readFrom(in);
            return item;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            id = in.readString();
            routing = in.readOptionalString();
            int assignmentsSize = in.readVInt();
            if (assignmentsSize > 0) {
                assignments = new Symbol[assignmentsSize];
                for (int i = 0; i < assignmentsSize; i++) {
                    assignments[i] = Symbol.fromStream(in);
                }
            }
            int missingAssignmentsSize = in.readVInt();
            if (missingAssignmentsSize > 0) {
                this.missingAssignments = new Object[missingAssignmentsSize];
                for (int i = 0; i < missingAssignmentsSize; i++) {
                    missingAssignments[i] = streamers[i].readValueFrom(in);
                }
            }

            version = Versions.readVersion(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(id);
            out.writeOptionalString(routing);
            if (assignments != null) {
                out.writeVInt(assignments.length);
                for (int i = 0; i < assignments.length; i++) {
                    Symbol.toStream(assignments[i], out);
                }
            } else {
                out.writeVInt(0);
            }
            // Stream References
            if (missingAssignments != null) {
                out.writeVInt(missingAssignments.length);
                for (int i = 0; i < missingAssignments.length; i++) {
                    streamers[i].writeValueTo(out, missingAssignments[i]);
                }
            } else {
                out.writeVInt(0);
            }

            Versions.writeVersion(version, out);
        }
    }

    private int shardId;
    private List<Item> items;
    private String[] assignmentsColumns;
    private IntArrayList locations;
    @Nullable
    private Reference[] missingAssignmentsColumns;
    @Nullable
    private Streamer[] streamers;
    private boolean continueOnError = false;

    public ShardUpsertRequest() {
    }

    public ShardUpsertRequest(String index,
                              int shardId,
                              @Nullable String[] assignmentsColumns,
                              @Nullable Reference[] missingAssignmentsColumns) {
        this.index = index;
        this.shardId = shardId;
        locations = new IntArrayList();
        this.assignmentsColumns = assignmentsColumns;
        this.missingAssignmentsColumns = missingAssignmentsColumns;
        items = new ArrayList<>();
        if (missingAssignmentsColumns != null) {
            streamers = new Streamer[missingAssignmentsColumns.length];
            for (int i = 0; i < missingAssignmentsColumns.length; i++) {
                streamers[i] = missingAssignmentsColumns[i].valueType().streamer();
            }
        }
    }

    public ShardUpsertRequest(ShardId shardId,
                              String[] assignmentsColumns,
                              @Nullable Reference[] missingAssignmentsColumns) {
        this(shardId.getIndex(), shardId.id(), assignmentsColumns, missingAssignmentsColumns);
    }

    public List<Item> items() {
        return items;
    }

    public IntArrayList locations() {
        return locations;
    }

    public ShardUpsertRequest add(int location,
                                  String id,
                                  @Nullable Symbol[] assignments,
                                  @Nullable Object[] missingAssignments,
                                  @Nullable Long version,
                                  @Nullable String routing) {
        locations.add(location);
        items.add(new Item(id, assignments, missingAssignments, version, routing, streamers));
        return this;
    }

    public ShardUpsertRequest add(int location,
                                  String id,
                                  Symbol[] assignments,
                                  @Nullable Long version,
                                  @Nullable String routing) {
        add(location, id, assignments, null, version, routing);
        return this;
    }

    public ShardUpsertRequest add(int location,
                                  String id,
                                  Object[] missingAssignments,
                                  @Nullable String routing) {
        add(location, id, null, missingAssignments, null, routing);
        return this;
    }

    public String type() {
        return Constants.DEFAULT_MAPPING_TYPE;
    }

    public int shardId() {
        return shardId;
    }

    public String[] assignmentsColumns() {
        return assignmentsColumns;
    }

    @Nullable
    public Reference[] missingAssignmentsColumns() {
        return missingAssignmentsColumns;
    }

    public boolean continueOnError() {
        return continueOnError;
    }

    public void continueOnError(boolean continueOnError) {
        this.continueOnError = continueOnError;
    }

    @Override
    public Iterator<Item> iterator() {
        return Iterators.unmodifiableIterator(items.iterator());
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        shardId = in.readInt();
        int assignmentsColumnsSize = in.readVInt();
        if (assignmentsColumnsSize > 0) {
            assignmentsColumns = new String[assignmentsColumnsSize];
            for (int i = 0; i < assignmentsColumnsSize; i++) {
                assignmentsColumns[i] = in.readString();
            }
        }
        int missingAssignmentsColumnsSize = in.readVInt();
        if (missingAssignmentsColumnsSize > 0) {
            missingAssignmentsColumns = new Reference[missingAssignmentsColumnsSize];
            streamers = new Streamer[missingAssignmentsColumnsSize];
            for (int i = 0; i < missingAssignmentsColumnsSize; i++) {
                missingAssignmentsColumns[i] = Reference.fromStream(in);
                streamers[i] = missingAssignmentsColumns[i].valueType().streamer();
            }
        }
        int size = in.readVInt();
        locations = new IntArrayList(size);
        items = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            locations.add(in.readVInt());
            items.add(Item.readItem(in, streamers));
        }
        continueOnError = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeInt(shardId);
        // Stream References
        if (assignmentsColumns != null) {
            out.writeVInt(assignmentsColumns.length);
            for(String column : assignmentsColumns) {
                out.writeString(column);
            }
        } else {
            out.writeVInt(0);
        }
        if (missingAssignmentsColumns != null) {
            out.writeVInt(missingAssignmentsColumns.length);
            for(Reference reference : missingAssignmentsColumns) {
                Reference.toStream(reference, out);
            }
        } else {
            out.writeVInt(0);
        }
        out.writeVInt(locations.size());
        for (int i = 0; i < locations.size(); i++) {
            out.writeVInt(locations.get(i));
            items.get(i).writeTo(out);
        }
        out.writeBoolean(continueOnError);
    }

}
