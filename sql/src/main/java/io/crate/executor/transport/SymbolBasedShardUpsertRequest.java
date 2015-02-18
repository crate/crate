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

public class SymbolBasedShardUpsertRequest extends ShardReplicationOperationRequest<SymbolBasedShardUpsertRequest> implements Iterable<SymbolBasedShardUpsertRequest.Item> {

    /**
     * A single update item.
     */
    static class Item implements Streamable {

        private String id;
        private String routing;
        private long version = Versions.MATCH_ANY;

        /**
         * List of symbols used on update if document exist
         */
        @Nullable
        private Symbol[] updateAssignments;

        /**
         * List of objects used on insert
         */
        @Nullable
        private Object[] insertValues;

        /**
         * List of data type streamer needed for streaming insert values
         */
        @Nullable
        private Streamer[] insertValuesStreamer;


        Item(@Nullable Streamer[] insertValuesStreamer) {
            this.insertValuesStreamer = insertValuesStreamer;
        }

        Item(String id,
             @Nullable Symbol[] updateAssignments,
             @Nullable Object[] insertValues,
             @Nullable Long version,
             @Nullable String routing,
             @Nullable Streamer[] insertValuesStreamer) {
            this(insertValuesStreamer);
            this.id = id;
            this.routing = routing;
            this.updateAssignments = updateAssignments;
            if (version != null) {
                this.version = version;
            }
            this.insertValues = insertValues;
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
        public Symbol[] updateAssignments() {
            return updateAssignments;
        }

        @Nullable
        public Object[] insertValues() {
            return insertValues;
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
                updateAssignments = new Symbol[assignmentsSize];
                for (int i = 0; i < assignmentsSize; i++) {
                    updateAssignments[i] = Symbol.fromStream(in);
                }
            }
            int missingAssignmentsSize = in.readVInt();
            if (missingAssignmentsSize > 0) {
                this.insertValues = new Object[missingAssignmentsSize];
                for (int i = 0; i < missingAssignmentsSize; i++) {
                    insertValues[i] = insertValuesStreamer[i].readValueFrom(in);
                }
            }

            version = Versions.readVersion(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(id);
            out.writeOptionalString(routing);
            if (updateAssignments != null) {
                out.writeVInt(updateAssignments.length);
                for (Symbol updateAssignment : updateAssignments) {
                    Symbol.toStream(updateAssignment, out);
                }
            } else {
                out.writeVInt(0);
            }
            // Stream References
            if (insertValues != null) {
                out.writeVInt(insertValues.length);
                for (int i = 0; i < insertValues.length; i++) {
                    insertValuesStreamer[i].writeValueTo(out, insertValues[i]);
                }
            } else {
                out.writeVInt(0);
            }

            Versions.writeVersion(version, out);
        }
    }

    private int shardId;
    private List<Item> items;
    private IntArrayList locations;
    private boolean continueOnError = false;
    private boolean overwriteDuplicates = false;

    /**
     * List of column names used on update
     */
    @Nullable
    private String[] updateColumns;

    /**
     * List of references used on insert
     */
    @Nullable
    private Reference[] insertColumns;

    /**
     * List of data type streamer resolved through insertColumns
     */
    @Nullable
    private Streamer[] insertValuesStreamer;

    public SymbolBasedShardUpsertRequest() {
    }

    public SymbolBasedShardUpsertRequest(ShardId shardId,
                                         @Nullable
                                         String[] updateColumns,
                                         @Nullable Reference[] insertColumns) {
        assert updateColumns != null || insertColumns != null
                : "Missing updateAssignments, whether for update nor for insert";
        this.index = shardId.getIndex();
        this.shardId = shardId.id();
        locations = new IntArrayList();
        this.updateColumns = updateColumns;
        this.insertColumns = insertColumns;
        items = new ArrayList<>();
        if (insertColumns != null) {
            insertValuesStreamer = new Streamer[insertColumns.length];
            for (int i = 0; i < insertColumns.length; i++) {
                insertValuesStreamer[i] = insertColumns[i].valueType().streamer();
            }
        }
    }

    public List<Item> items() {
        return items;
    }

    public IntArrayList locations() {
        return locations;
    }

    public SymbolBasedShardUpsertRequest add(int location,
                                  String id,
                                  @Nullable Symbol[] assignments,
                                  @Nullable Object[] missingAssignments,
                                  @Nullable Long version,
                                  @Nullable String routing) {
        locations.add(location);
        items.add(new Item(id, assignments, missingAssignments, version, routing, insertValuesStreamer));
        return this;
    }

    public SymbolBasedShardUpsertRequest add(int location,
                                  String id,
                                  Symbol[] assignments,
                                  @Nullable Long version,
                                  @Nullable String routing) {
        add(location, id, assignments, null, version, routing);
        return this;
    }

    public String type() {
        return Constants.DEFAULT_MAPPING_TYPE;
    }

    public int shardId() {
        return shardId;
    }

    public String[] updateColumns() {
        return updateColumns;
    }

    @Nullable
    public Reference[] insertColumns() {
        return insertColumns;
    }

    public boolean overwriteDuplicates() {
        return overwriteDuplicates;
    }

    public void overwriteDuplicates(boolean overwriteDuplicates) {
        this.overwriteDuplicates = overwriteDuplicates;
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
            updateColumns = new String[assignmentsColumnsSize];
            for (int i = 0; i < assignmentsColumnsSize; i++) {
                updateColumns[i] = in.readString();
            }
        }
        int missingAssignmentsColumnsSize = in.readVInt();
        if (missingAssignmentsColumnsSize > 0) {
            insertColumns = new Reference[missingAssignmentsColumnsSize];
            insertValuesStreamer = new Streamer[missingAssignmentsColumnsSize];
            for (int i = 0; i < missingAssignmentsColumnsSize; i++) {
                insertColumns[i] = Reference.fromStream(in);
                insertValuesStreamer[i] = insertColumns[i].valueType().streamer();
            }
        }
        int size = in.readVInt();
        locations = new IntArrayList(size);
        items = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            locations.add(in.readVInt());
            items.add(Item.readItem(in, insertValuesStreamer));
        }
        continueOnError = in.readBoolean();
        overwriteDuplicates = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeInt(shardId);
        // Stream References
        if (updateColumns != null) {
            out.writeVInt(updateColumns.length);
            for(String column : updateColumns) {
                out.writeString(column);
            }
        } else {
            out.writeVInt(0);
        }
        if (insertColumns != null) {
            out.writeVInt(insertColumns.length);
            for(Reference reference : insertColumns) {
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
        out.writeBoolean(overwriteDuplicates);
    }

}
