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
import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.cursors.IntCursor;
import com.carrotsearch.hppc.cursors.LongCursor;
import com.google.common.collect.UnmodifiableIterator;
import io.crate.Constants;
import io.crate.core.collections.Row;
import io.crate.core.collections.Rows;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.types.DataType;
import org.elasticsearch.action.support.replication.ShardReplicationOperationRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.*;

public class ShardUpsertRequest extends ShardReplicationOperationRequest<ShardUpsertRequest> implements Iterable<ShardUpsertRequest.Item> {

    private final Item item = new Item();

    private int shardId;
    private IntArrayList locations;
    private List<String> ids;
    private LongArrayList versions;
    private boolean continueOnError = false;
    private boolean overwriteDuplicates = false;

    @Nullable
    private String routing;

    /**
     * Map of references and symbols used on update if document exist
     */
    @Nullable
    private Map<Reference, Symbol> updateAssignments;

    /**
     * Map of references and symbols used on insert
     */
    @Nullable
    private Map<Reference, Symbol> insertAssignments;

    private Rows rows;

    public ShardUpsertRequest() {
    }

    public ShardUpsertRequest(ShardId shardId,
                              DataType[] dataTypes,
                              List<Integer> columnIndicesToStream,
                              @Nullable Map<Reference, Symbol> updateAssignments,
                              @Nullable Map<Reference, Symbol> insertAssignments) {
        this(shardId, dataTypes, columnIndicesToStream, updateAssignments, insertAssignments, null);
    }

    public ShardUpsertRequest(ShardId shardId,
                              DataType[] dataTypes,
                              List<Integer> columnIndicesToStream,
                              @Nullable Map<Reference, Symbol> updateAssignments,
                              @Nullable Map<Reference, Symbol> insertAssignments,
                              @Nullable String routing) {
        assert updateAssignments != null || insertAssignments != null
                : "Missing assignments, whether for update nor for insert given";
        this.index = shardId.getIndex();
        this.shardId = shardId.id();
        this.routing = routing;
        this.updateAssignments = updateAssignments;
        this.insertAssignments = insertAssignments;
        locations = new IntArrayList();
        ids = new ArrayList<>();
        versions = new LongArrayList();
        rows = new Rows(dataTypes, columnIndicesToStream);
    }

    @Nullable
    public String routing() {
        return routing;
    }

    public IntArrayList locations() {
        return locations;
    }

    public ShardUpsertRequest add(int location,
                                  String id,
                                  Object[] row,
                                  @Nullable Long version,
                                  @Nullable String routing) {
        locations.add(location);
        ids.add(id);
        rows.add(row);
        if (version != null) {
            versions.add(version);
        } else {
            versions.add(Versions.MATCH_ANY);
        }
        if (this.routing == null) {
            this.routing = routing;
        }
        return this;
    }

    public String type() {
        return Constants.DEFAULT_MAPPING_TYPE;
    }

    public int shardId() {
        return shardId;
    }

    @Nullable
    public Map<Reference, Symbol> updateAssignments() {
        return updateAssignments;
    }

    @Nullable
    public Map<Reference, Symbol> insertAssignments() {
        return insertAssignments;
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
        return new UnmodifiableIterator<Item>() {
            private Iterator<IntCursor> locationsIterator = locations.iterator();
            private Iterator<String> idsIterator = ids.iterator();
            private Iterator<Row> rowsIterator = rows.iterator();
            private Iterator<LongCursor> versionsIterator = versions.iterator();

            @Override
            public boolean hasNext() {
                return rowsIterator.hasNext();
            }

            @Override
            public Item next() {
                item.location = locationsIterator.next().value;
                item.id = idsIterator.next();
                item.row = rowsIterator.next();
                item.version = versionsIterator.next().value;
                return item;
            }
        };
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        shardId = in.readInt();
        routing = in.readOptionalString();
        int updateAssignmentsSize = in.readVInt();
        if (updateAssignmentsSize > 0) {
            updateAssignments = new HashMap<>();
            for (int i = 0; i < updateAssignmentsSize; i++) {
                updateAssignments.put(Reference.fromStream(in), Symbol.fromStream(in));
            }
        }
        int insertAssignmentsSize = in.readVInt();
        if (insertAssignmentsSize > 0) {
            insertAssignments = new HashMap<>();
            for (int i = 0; i < insertAssignmentsSize; i++) {
                insertAssignments.put(Reference.fromStream(in), Symbol.fromStream(in));
            }
        }

        int size = in.readVInt();
        locations = new IntArrayList(size);
        ids = new ArrayList<>(size);
        versions = new LongArrayList(size);
        for (int i = 0; i < size; i++) {
            locations.add(in.readVInt());
            ids.add(in.readString());
            versions.add(in.readLong());
        }
        rows = Rows.fromStream(in);

        continueOnError = in.readBoolean();
        overwriteDuplicates = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeInt(shardId);
        out.writeOptionalString(routing);
        // Stream assignment symbols
        if (updateAssignments != null) {
            out.writeVInt(updateAssignments.size());
            for (Map.Entry<Reference, Symbol> entry : updateAssignments.entrySet()) {
                Reference.toStream(entry.getKey(), out);
                Symbol.toStream(entry.getValue(), out);
            }
        } else {
            out.writeVInt(0);
        }
        if (insertAssignments != null) {
            out.writeVInt(insertAssignments.size());
            for (Map.Entry<Reference, Symbol> entry : insertAssignments.entrySet()) {
                Reference.toStream(entry.getKey(), out);
                Symbol.toStream(entry.getValue(), out);
            }
        } else {
            out.writeVInt(0);
        }

        out.writeVInt(locations.size());
        for (int i = 0; i < locations.size(); i++) {
            out.writeVInt(locations.get(i));
            out.writeString(ids.get(i));
            out.writeLong(versions.get(i));
        }
        rows.writeTo(out);

        out.writeBoolean(continueOnError);
        out.writeBoolean(overwriteDuplicates);
    }


    /**
     * A single update item.
     */
    public static class Item {

        private int location;
        private String id;
        private Row row;
        private long version = Versions.MATCH_ANY;

        Item() {
        }

        public int location() {
            return location;
        }

        public String id() {
            return id;
        }

        public Row row() {
            return row;
        }

        public long version() {
            return version;
        }

        public int retryOnConflict() {
            return version == Versions.MATCH_ANY ? Constants.UPDATE_RETRY_ON_CONFLICT : 0;
        }
    }

}
