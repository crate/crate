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

import com.google.common.base.Objects;
import io.crate.Constants;
import io.crate.Streamer;
import io.crate.analyze.symbol.Reference;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.doc.DocSysColumns;
import org.elasticsearch.Version;
import org.elasticsearch.action.bulk.BulkShardProcessor;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;

public class ShardUpsertRequest extends ShardRequest<ShardUpsertRequest, ShardUpsertRequest.Item> {

    private boolean continueOnError = false;
    private boolean overwriteDuplicates = false;
    private Boolean isRawSourceInsert = null;
    private boolean validateGeneratedColumns = true;

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

    public ShardUpsertRequest() {
    }

    public ShardUpsertRequest(ShardId shardId,
                              @Nullable String[] updateColumns,
                              @Nullable Reference[] insertColumns,
                              @Nullable String routing,
                              UUID jobId) {
        super(shardId, routing, jobId);
        assert updateColumns != null || insertColumns != null
                : "Missing updateAssignments, whether for update nor for insert";
        this.updateColumns = updateColumns;
        this.insertColumns = insertColumns;
        if (insertColumns != null) {
            insertValuesStreamer = new Streamer[insertColumns.length];
            for (int i = 0; i < insertColumns.length; i++) {
                insertValuesStreamer[i] = insertColumns[i].valueType().streamer();
            }
        }
    }

    public void add(int location, Item item) {
        item.insertValuesStreamer(insertValuesStreamer);
        super.add(location, item);
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

    public ShardUpsertRequest overwriteDuplicates(boolean overwriteDuplicates) {
        this.overwriteDuplicates = overwriteDuplicates;
        return this;
    }

    public boolean continueOnError() {
        return continueOnError;
    }

    public ShardUpsertRequest continueOnError(boolean continueOnError) {
        this.continueOnError = continueOnError;
        return this;
    }

    public boolean validateGeneratedColumns() {
        return validateGeneratedColumns;
    }

    public ShardUpsertRequest validateGeneratedColumns(boolean validateGeneratedColumns) {
        this.validateGeneratedColumns = validateGeneratedColumns;
        return this;
    }

    public Boolean isRawSourceInsert() {
        if (isRawSourceInsert == null) {
            isRawSourceInsert =
                    insertColumns.length == 1 && insertColumns[0].ident().columnIdent().equals(DocSysColumns.RAW);
        }
        return isRawSourceInsert;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
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
        continueOnError = in.readBoolean();
        overwriteDuplicates = in.readBoolean();
        validateGeneratedColumns = in.readBoolean();
        readItems(in, locations.size());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        // Stream References
        if (updateColumns != null) {
            out.writeVInt(updateColumns.length);
            for (String column : updateColumns) {
                out.writeString(column);
            }
        } else {
            out.writeVInt(0);
        }
        if (insertColumns != null) {
            out.writeVInt(insertColumns.length);
            for (Reference reference : insertColumns) {
                Reference.toStream(reference, out);
            }
        } else {
            out.writeVInt(0);
        }
        out.writeBoolean(continueOnError);
        out.writeBoolean(overwriteDuplicates);
        out.writeBoolean(validateGeneratedColumns);
        writeItems(out);
    }

    @Override
    protected Item readItem(StreamInput input) throws IOException {
        return Item.readItem(input, insertValuesStreamer);
    }

    @Override
    public boolean equals(Object o) {
        if (!super.equals(o)) return false;
        if (this == o) return true;
        if (getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        ShardUpsertRequest items = (ShardUpsertRequest) o;
        return continueOnError == items.continueOnError &&
               overwriteDuplicates == items.overwriteDuplicates &&
               validateGeneratedColumns == items.validateGeneratedColumns &&
               Objects.equal(isRawSourceInsert, items.isRawSourceInsert) &&
               Arrays.equals(updateColumns, items.updateColumns) &&
               Arrays.equals(insertColumns, items.insertColumns) &&
               Arrays.equals(insertValuesStreamer, items.insertValuesStreamer);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(super.hashCode(), continueOnError, overwriteDuplicates, isRawSourceInsert, validateGeneratedColumns, updateColumns, insertColumns, insertValuesStreamer);
    }

    /**
     * A single update item.
     */
    public static class Item extends ShardRequest.Item {

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

        protected Item() {
        }

        public Item(String id,
                    @Nullable Symbol[] updateAssignments,
                    @Nullable Object[] insertValues,
                    @Nullable Long version) {
            super(id);
            this.updateAssignments = updateAssignments;
            if (version != null) {
                this.version = version;
            }
            this.insertValues = insertValues;
        }

        public void insertValuesStreamer(@Nullable Streamer[] insertValuesStreamer) {
            this.insertValuesStreamer = insertValuesStreamer;
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

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Item item = (Item) o;
            return version == item.version &&
                   Objects.equal(id, item.id) &&
                   Arrays.deepEquals(updateAssignments, item.updateAssignments) &&
                   Arrays.deepEquals(insertValues, item.insertValues) &&
                   Arrays.equals(insertValuesStreamer, item.insertValuesStreamer);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(id, version, updateAssignments, insertValues, insertValuesStreamer);
        }

        static Item readItem(StreamInput in, @Nullable Streamer[] streamers) throws IOException {
            Item item = new Item();
            item.insertValuesStreamer(streamers);
            item.readFrom(in);
            return item;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            id = in.readString();
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

            version = Version.readVersion(in).id;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(id);
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

            Version.writeVersion(Version.fromId((int)version), out);
        }
    }

    public static class Builder implements BulkShardProcessor.BulkRequestBuilder<ShardUpsertRequest> {

        private final TimeValue timeout;
        private final boolean overwriteDuplicates;
        private final boolean continueOnError;
        @Nullable
        private final String[] assignmentsColumns;
        @Nullable
        private final Reference[] missingAssignmentsColumns;
        private final UUID jobId;
        private boolean validateGeneratedColumns;

        public Builder(TimeValue timeout,
                       boolean overwriteDuplicates,
                       boolean continueOnError,
                       @Nullable String[] assignmentsColumns,
                       @Nullable Reference[] missingAssignmentsColumns,
                       UUID jobId) {
            this(timeout, overwriteDuplicates, continueOnError, assignmentsColumns, missingAssignmentsColumns, jobId, true);
        }

        public Builder(TimeValue timeout,
                       boolean overwriteDuplicates,
                       boolean continueOnError,
                       @Nullable String[] assignmentsColumns,
                       @Nullable Reference[] missingAssignmentsColumns,
                       UUID jobId,
                       boolean validateGeneratedColumns) {
            this.timeout = timeout;
            this.overwriteDuplicates = overwriteDuplicates;
            this.continueOnError = continueOnError;
            this.assignmentsColumns = assignmentsColumns;
            this.missingAssignmentsColumns = missingAssignmentsColumns;
            this.jobId = jobId;
            this.validateGeneratedColumns = validateGeneratedColumns;
        }

        @Override
        public ShardUpsertRequest newRequest(ShardId shardId, String routing) {
            return new ShardUpsertRequest(
                    shardId,
                    assignmentsColumns,
                    missingAssignmentsColumns,
                    routing,
                    jobId)
                    .timeout(timeout)
                    .continueOnError(continueOnError)
                    .overwriteDuplicates(overwriteDuplicates)
                    .validateGeneratedColumns(validateGeneratedColumns);
        }
    }
}
